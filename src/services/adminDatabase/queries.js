const admDBAccess = require('./access');
const admDBApp = require('./connectionApp');

const saldoExport = async (cdCliente, dataInicio, _dataFim, isApi = false) => {

  const dataFim = _dataFim.trim().length == 10 ? `${_dataFim.trim()} 23:59:59` : _dataFim;
  let select = `with tmp_cnpjs_evento as (
      select jsonb_array_elements_text(empresas_json) as cnpj_evento,
             case
                 when
                     (to_date((dt_evento at time zone 'GMT -3')::text, 'yyyy-mm-dd') >= '${dataInicio}'::date  
         and to_date((dt_evento at time zone 'GMT -3')::text, 'yyyy-mm-dd') <= '${dataFim}'::date)
                     then true
                 else false end                       as flag_mes_atual,
             cd_evento,
             dt_evento
      from public.evento_cliente
      where cd_cliente = '${cdCliente}'
        and tp_evento in ('EXPORT', 'EXP-EXACTSALES', 'EXPORT-API')
        and to_date((dt_evento at time zone 'GMT -3')::text, 'yyyy-mm-dd') <= '${dataFim}'
  ),
   tmp_cnpjs_flag as (
       select cnpj_evento,
              count(cnpj_evento) filter (where flag_mes_atual is true) as qtd_flag_true, count(cnpj_evento) filter (where flag_mes_atual is false) as qtd_flag_false
       from tmp_cnpjs_evento
       group by cnpj_evento
   )`;

  if (isApi) {
    select += `, saldo_calculado as (select count(distinct ce.cnpj_evento) as saldo
        from tmp_cnpjs_evento ce
        inner join tmp_cnpjs_flag cf on cf.cnpj_evento = ce.cnpj_evento and (cf.qtd_flag_true <> 0 and cf.qtd_flag_false = 0)
        where ce.flag_mes_atual is true)
        select sc.saldo, pc.qtd_plano_limite_exportacoes
        from saldo_calculado sc
        inner join public.plano_cliente pc on pc.cd_cliente = '${cdCliente}'
      `;
  } else {
    select += `select count(distinct ce.cnpj_evento) as saldo
               from tmp_cnpjs_evento ce
                        inner join tmp_cnpjs_flag cf on cf.cnpj_evento = ce.cnpj_evento and
                                                        (cf.qtd_flag_true <> 0 and cf.qtd_flag_false = 0)
               where ce.flag_mes_atual is true`;
  }

  return (await admDBAccess.executeQuery(select))[0].saldo;
};

const saldoEmail = async (cdCliente, startDate, endDate) => {
  let query = `
      with tmp_qtd_pendentes as (
          select coalesce(count(empresas_json ->> 0), 0) as total_pendentes,
                 $1                                      as cd_cliente
          from public.evento_cliente_validacao_email
          where cd_cliente = $1
            and status = 'APPROVED'
            and to_date((dt_evento at time zone 'GMT -3')::text, 'yyyy-mm-dd') >= $2
          :: date
          and to_date((dt_evento at time zone 'GMT -3')::text
         , 'ýyyy-mm-dd') <= $3:: date
          and lote not in (
          select distinct lote
          from public.evento_cliente_validacao_email
          where (status = 'SUCCESS' or status = 'NOT_FOUND')
          and cd_cliente = $1 )
          )
         , tmp_lotes_success as (
      select distinct lote as lote_success
      from public.evento_cliente_validacao_email
      where cd_cliente = $1
        and status = 'APPROVED'
        and to_date((dt_evento at time zone 'GMT -3')::text
          , 'yyyy-mm-dd') >= $2:: date
        and to_date((dt_evento at time zone 'GMT -3')::text
          , 'ýyyy-mm-dd') <= $3:: date
        and lote in (
          select distinct lote
          from public.evento_cliente_validacao_email
          where status = 'SUCCESS'
        and cd_cliente = $1)
          )
          , tmp_qtd_success as (
      select coalesce (count (distinct empresas_json ->> 0), 0) as total_success,
          $1 as cd_cliente
      from tmp_lotes_success tlc
          inner join public.evento_cliente_validacao_email ec
      on ec.lote = tlc.lote_success
      where ec.status = 'SUCCESS'
          )
      select (total_pendentes + total_success) as saldo
      from public.plano_cliente pc
               inner join tmp_qtd_pendentes tp on pc.cd_cliente = tp.cd_cliente
               inner join tmp_qtd_success ts on pc.cd_cliente = ts.cd_cliente
      where pc.cd_cliente = $1 `;
  const params = [cdCliente, startDate, endDate]

  return (await admDBAccess.executeQuery(query, params))[0].saldo;
}

const saldoPhone = async (cdCliente, dtBegin, dtEnd) => {
  const queryCompleted = `SELECT count(DISTINCT public.phone_validation.cnpj) as completed_count
                          FROM public.phone_validation
                                   JOIN public.phone_validation_event
                                        ON public.phone_validation_event.id = public.phone_validation.event_id
                          WHERE public.phone_validation_event.cd_cliente = $1
                            AND public.phone_validation_event.date_event BETWEEN $2 AND $3
                            AND public.phone_validation_event.status_event = 'COMPLETED'
                            and public.phone_validation.status_validation IN ('completed', 'busy', 'no-answer')`;

  const queryProcessing = `SELECT sum(qtd_empresas) as processing_count
                           from public.phone_validation_event
                           where cd_cliente = $1
                             AND public.phone_validation_event.date_event BETWEEN $2 AND $3
                             and status_event = 'PROCESSING'`;
  const params = [cdCliente, dtBegin, dtEnd];
  let loteCompleto = await admDBAccess.executeQuery(queryCompleted, params);
  let loteEmProcessamento = await admDBAccess.executeQuery(queryProcessing, params);

  if (loteEmProcessamento[0].processing_count) {
    return parseInt(loteCompleto[0].completed_count) + parseInt(loteEmProcessamento[0].processing_count);
  }
  return parseInt(loteCompleto[0].completed_count);

};

const lastDay = function (y, m) {
  return new Date(y, m + 1, 0).getDate();
}

async function getPlanoCliente(cdCliente) {
  const queryPlanoCliente = `SELECT vigencia,
                                    qtd_plano_limite_exportacoes,
                                    qtd_plano_limite_valida_email,
                                    qtd_plano_limite_valida_tel
                             FROM plano_cliente
                             WHERE public.plano_cliente.cd_cliente = $1`;

  const params = [cdCliente];

  return (await admDBAccess.executeQuery(queryPlanoCliente, params))[0];
}

const generateBalance = async (cdCliente) => {

  let planoCliente = await getPlanoCliente(cdCliente);

  let months = {
    'export': {},
    'email': {},
    'phone': {},
  };

  for (let i = 0; i < 11; i++) {
    months['export'][i] = await saldoExport(cdCliente, '2021-' + (parseInt(i) + 1) + '-01', '2021-' + (parseInt(i) + 1) + '-' + lastDay('2021', i))
    months['email'][i] = await saldoEmail(cdCliente, '2021-' + (parseInt(i) + 1) + '-01', '2021-' + (parseInt(i) + 1) + '-' + lastDay('2021', i))
    months['phone'][i] = await saldoPhone(cdCliente, '2021-' + (parseInt(i) + 1) + '-01', '2021-' + (parseInt(i) + 1) + '-' + lastDay('2021', i))

    await adicionarSaldo(
      cdCliente,
      2021,
      i,
      parseInt(planoCliente.qtd_plano_limite_exportacoes),
      parseInt(planoCliente.qtd_plano_limite_valida_email),
      parseInt(planoCliente.qtd_plano_limite_valida_tel),
      parseInt(months['export'][i]),
      parseInt(months['email'][i]),
      parseInt(months['phone'][i]));
  }

  return months;

}

const adicionarSaldo = async (cdCliente, ano, mes, saldoExport, saldoEmail, saldoPhone, saldoExpDebitado = 0, saldoEmailDebitado = 0, saldoPhoneDebitado = 0) => {
  const query = `INSERT INTO public.plano_cliente_saldo (cd_cliente, ano, mes, exp_saldo, email_saldo, tel_saldo,
                                                         exp_saldo_debitado, email_saldo_debitado, tel_saldo_debitado)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING * `;

  const params = [cdCliente, ano, mes, saldoExport, saldoEmail, saldoPhone, saldoExpDebitado, saldoEmailDebitado, saldoPhoneDebitado];

  return (await admDBAccess.executeQuery(query, params))[0];
};

const checkBalance = async (cdCliente, saldoCliente, saldo_debitado, saldo, saldo_bonus, amount, place = 1, queryText = '') => {

  //se existe o mes no saldo
  if (saldoCliente[saldoCliente.length - place]) {

    let clienteElement = saldoCliente[saldoCliente.length - place];

    // se o saldo debitado + a quantidade que vai ser debitada for menor que o saldo, debita do mes e tchau
    if ((clienteElement[saldo_debitado] + amount) <= (clienteElement[saldo]+clienteElement[saldo_bonus])) {
      // saldoCliente[0].exp_saldo_debitado + amount
      queryText += `UPDATE public.plano_cliente_saldo
                    SET ${saldo_debitado} = ${(clienteElement[saldo_debitado] + amount)}
                    WHERE cd_cliente = '${cdCliente}'
                      AND ano = ${clienteElement['ano']}
                      AND mes = ${clienteElement['mes']};`;
      return queryText;
    } else {

      if (saldoCliente[saldoCliente.length - (place + 1)]) {
        //else complica
        let debitoMesAtual = (clienteElement[saldo]+clienteElement[saldo_bonus]) - clienteElement[saldo_debitado];
        let debitoMesSeguinte = amount - ((clienteElement[saldo]+clienteElement[saldo_bonus]) - (clienteElement[saldo_debitado]));
        //chama a recursao
        queryText += `UPDATE public.plano_cliente_saldo
                      SET ${saldo_debitado} =
                              ${(clienteElement[saldo_debitado] + debitoMesAtual)}
                      WHERE cd_cliente = '${cdCliente}'
                        AND ano = ${clienteElement['ano']}
                        AND mes = ${clienteElement['mes']};`;
        return await checkBalance(cdCliente, saldoCliente, saldo_debitado, saldo, saldo_bonus, debitoMesSeguinte, (place + 1), queryText);

      } else {
        //saldo inválido
        return false;
      }
    }
  } else {
    return false;
  }

}

const returnBalance = async (cdCliente, tpEvento, amount) => {
  /**
   * 'saldoCliente' é um array ordenado, do mês mais antigo para o mais novo, contendo objetos que correspondem às
   *   linhas da tabela plano_cliente_saldo para o cliente, e contém apenas os meses correspondentes ao valor de vigência do cliente.
   */
  let saldoCliente = await saldo(cdCliente);
  saldoCliente.reverse()

  let tpLog, saldo_debitado_col;
  if (tpEvento === 'phone') {
    tpLog = 'tel-return';
    saldo_debitado_col = 'tel_saldo_debitado'
  } else if (tpEvento === 'email') {
    tpLog = 'email-return';
    saldo_debitado_col = 'email_saldo_debitado';
  } else {
    throw new Error('tpEvento inválido.');
  }

  while (saldoCliente.length > 0 && amount > 0) {
    const linha = saldoCliente.shift();
    if (linha[saldo_debitado_col] > 0) {
      const retirar = Math.min(linha[saldo_debitado_col], amount);
      amount -= retirar;
      const query = `
        UPDATE public.plano_cliente_saldo
        SET ${saldo_debitado_col} = $1
        WHERE (
          ( cd_cliente = $2 ) AND
          ( ano = $3 ) AND
          ( mes = $4 )
        );
      `;
      admDBAccess.executeQuery(query, [linha[saldo_debitado_col] - retirar, cdCliente, linha.ano, linha.mes]);
    }
  }
  
  return 'success';
};

const removeBalance = async (cdCliente, tpEvento, amount) => {
  let saldoCliente = await saldo(cdCliente);
  let isValid = false;
  if (tpEvento.toLowerCase() === 'export') {
    isValid = await checkBalance(cdCliente, saldoCliente, 'exp_saldo_debitado', 'exp_saldo', 'exp_bonus', parseInt(amount))
  }
  if (tpEvento.toLowerCase() === 'phone') {
    isValid = await checkBalance(cdCliente, saldoCliente, 'tel_saldo_debitado', 'tel_saldo', 'tel_bonus', parseInt(amount))
  }
  if (tpEvento.toLowerCase() === 'email') {
    isValid = await checkBalance(cdCliente, saldoCliente, 'email_saldo_debitado', 'email_saldo', 'email_bonus', parseInt(amount))
  }
  if (isValid){
    admDBAccess.executeQuery(isValid)
  }
  return !!isValid;
}

const saldo = async (cdCliente) => {
  let planoCliente = await getPlanoCliente(cdCliente);
  const query = `SELECT *
                 FROM plano_cliente_saldo
                 WHERE public.plano_cliente_saldo.cd_cliente = $1
                 ORDER BY (ano, mes)
                 DESC
                 LIMIT $2`;
  const params = [cdCliente, planoCliente.vigencia];
  return (await admDBAccess.executeQuery(query, params));
};

module.exports = {
  saldoExport,
  saldoEmail,
  lastDay,
  generateBalance,
  adicionarSaldo,
  saldo,
  removeBalance,
  returnBalance,
  checkBalance,
};
