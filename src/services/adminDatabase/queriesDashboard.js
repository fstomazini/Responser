
const admDBAccess = require('./access');
const appDBAccess = require('./connectionApp.js');


async function ultimasEmpresasVisualizadas(info){
    const {
        cdCliente,
        nmUsuario,
        periodo
    } = info

    let query;
    let params = []

    if(nmUsuario != 'Todos os Usuários'){
        query = `SELECT lpde.dt_evento, lpde.cnpj, u.nm_usuario
        FROM logs.log_plat_detalhe_empresa lpde 
        INNER JOIN public.usuario u
        ON lpde.id_usuario = u.id_usuario
        WHERE u.nm_usuario = $1
        AND dt_evento >= (CURRENT_DATE - ${periodo})
        ORDER BY dt_evento DESC 
        LIMIT 50`;

        params = [nmUsuario]
    }
    else {
        query = `SELECT lpde.dt_evento, lpde.cnpj, u.nm_usuario 
        FROM logs.log_plat_detalhe_empresa lpde 
        INNER JOIN public.usuario u
        ON lpde.id_usuario = u.id_usuario
        WHERE lpde.cd_cliente = $1
        AND dt_evento >= (CURRENT_DATE - ${periodo}) 
        ORDER BY dt_evento DESC 
        LIMIT 50`;

        params = [cdCliente]
    }

    return await appDBAccess.executeQuery(query, params);;
}

async function ultimasExportacoesRealizadas(info){
    const {
        cdCliente,
        nmUsuario,
        periodo
    } = info;

    let query;
    let params;

    if(nmUsuario != 'Todos os Usuários'){
        query = `
            SELECT ec.cd_evento, ec.empresas_json, u.nm_usuario, ec.dt_evento
            FROM evento_cliente ec
            LEFT JOIN usuario u 
            ON ec.id_usuario = u.id_usuario
            WHERE u.nm_usuario = $1
            AND ec.dt_evento >= (current_date - ${periodo})
            order by ec.dt_evento desc limit 5
        `
        params = [nmUsuario]
    }
    else {
        query = `
            SELECT ec.cd_evento, ec.empresas_json, u.nm_usuario, ec.dt_evento
            FROM evento_cliente ec
            LEFT JOIN usuario u 
            ON ec.id_usuario = u.id_usuario
            WHERE ec.cd_cliente = $1
            AND ec.dt_evento >= (current_date - ${periodo})
            order by ec.dt_evento desc limit 5
        `
        params = [cdCliente]
    }

    return await appDBAccess.executeQuery(query,params);
}

async function topFiltrosCliente(cd_cliente, usuario, periodo){    
    let query = `select payload, usuario, origem from app_plataforma.analytics a where 
    data >= (CURRENT_DATE - ${periodo}) and origem = 'plataforma' and acao = 'botao-atualizar-pesquisa' and cd_cliente = '${cd_cliente}'`;

    if (usuario != 'Todos os Usuários') {
        query += ` and usuario = '${usuario}'`;
    }
   
    return (await appDBAccess.executeQuery(query));
}

async function telEnviadosValidacaoCliente(cd_cliente, usuario, periodo){    
    let query = `select cnpj, to_date((date_event at time zone 'GMT -3')::text, 'yyyy-mm-dd') as date_event, email_responsavel_lead, pv.status_validation from public.phone_validation_event pve join public.phone_validation pv on pve.id = pv.event_id 
    where date_event >= (CURRENT_DATE - ${periodo}) and pve.cd_cliente = '${cd_cliente}'`;
   
    if (usuario != 'Todos os Usuários') {
        query += ` and pve.email_responsavel_lead = '${usuario}'`;
    }

    return (await appDBAccess.executeQuery(query));
}

async function emailEnviadosValidacaoCliente(cd_cliente, usuario, periodo){
    let query = `select id_usuario, to_date((dt_evento at time zone 'GMT -3')::text, 'yyyy-mm-dd') as dt_evento, status, cd_cliente, lote, empresas_json from public.evento_cliente_validacao_email ecve where
    dt_evento >= (CURRENT_DATE - ${periodo}) and cd_cliente = '${cd_cliente}'`;
   
    if (usuario != 'Todos os Usuários') {
        query += ` and ds_email_responsavel_lead = '${usuario}'`;
    }

    return (await appDBAccess.executeQuery(query));
}

async function pesquisasCliente(cd_cliente, usuario, periodo){
    let query = `select cd_cliente, id_usuario, email, to_date((dt_criacao at time zone 'GMT -3')::text, 'yyyy-mm-dd') as dt_criacao, pesquisa_json, id_pesquisa, url_param from public.log_pesquisa lp 
    where dt_criacao >= (CURRENT_DATE - ${periodo}) and cd_cliente = '${cd_cliente}'`;
   
    if (usuario != 'Todos os Usuários') {
        query += ` and email = '${usuario}'`;
    }

    return (await appDBAccess.executeQuery(query));
}

async function detalheCliente(cd_cliente, usuario, periodo){
    let query = `select cnpj, to_date((lpde.dt_evento at time zone 'GMT -3')::text, 'yyyy-mm-dd') as dt_evento, cnpj, lpde.cd_cliente id_usuario, nm_usuario from logs.log_plat_detalhe_empresa lpde inner join public.usuario u on lpde.id_usuario = u.id_usuario 
    where lpde.dt_evento >= (CURRENT_DATE - ${periodo}) and lpde.cd_cliente = '${cd_cliente}'`;
   
    if (usuario != 'Todos os Usuários') {
        query += ` and u.nm_usuario = '${usuario}'`;
    }

    return (await appDBAccess.executeQuery(query));
}

async function exportacaoCliente(cd_cliente, usuario, periodo){
    let query = `select cd_evento, ec.cd_cliente , ec.id_usuario, to_date((ec.dt_evento at time zone 'GMT -3')::text, 'yyyy-mm-dd') as dt_evento, nm_usuario, qtd_empresas from public.evento_cliente ec inner join public.usuario u on ec.id_usuario  = u.id_usuario  
    where ec.dt_evento >= (current_date - ${periodo}) and ec.cd_cliente = '${cd_cliente}'`;
   
    if (usuario != 'Todos os Usuários') {
        query += ` and nm_usuario = '${usuario}'`;
    }

    return (await appDBAccess.executeQuery(query));
}

module.exports = {
    telEnviadosValidacaoCliente, 
    emailEnviadosValidacaoCliente,
    pesquisasCliente,
    detalheCliente,
    exportacaoCliente,
    topFiltrosCliente,
    ultimasEmpresasVisualizadas,
    ultimasExportacoesRealizadas
}