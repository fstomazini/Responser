const ecdtLibs = require('ecdt-libs');
const platAdminAccess = require('../services/adminDatabase/queriesDashboard');
const jwt = require('jsonwebtoken');
const axios = require("axios");

const returnDadosTabelasDashboard = async(req , res) =>{
  try{
    const tabelas = await getDadosTabelasDashboard(req.body.usuarios, req.body.cdCliente);
    return res.status(200).json(tabelas);
  }
  catch(erros) {
    console.log(erros);
    res.status(500).json({Erro: 'Não foi possível executar a solicitação.'});
  }
}

const procurador = async(req , res) =>{
  
  let ipCliente = req.connection.remoteAddress || req.socket.remoteAddress || req.connection.socket.remoteAddress;
  

  return res.body({httpResponse : "ok", htppStatus : 200 , ipCliente : ipCliente })
}

async function getDadosTabelasDashboard(usuarios, cd_cliente) {
  const dadosTabelaEmpresasVisualizadas = await platAdminAccess.ultimasEmpresasVisualizadas({cdCliente: cd_cliente, nmUsuario: usuarios, periodo: 90});
  const dadosTabelaEmpresasExportadas = await platAdminAccess.ultimasExportacoesRealizadas({cdCliente: cd_cliente, nmUsuario: usuarios, periodo: 90});
  
  let cnpjs = [];
  //As seguintes ações têm como objetivo preencher os nomes das empresas visualizadas
  dadosTabelaEmpresasVisualizadas.forEach(empresa => { cnpjs.push(empresa.cnpj) })

  let empresasElastic = await buscaNomesEmpresas(cnpjs);

  dadosTabelaEmpresasVisualizadas.forEach((empresa, index) => {
    let empresaEncontrada = empresasElastic.find(empresaElastic => empresaElastic.cnpj == empresa.cnpj);
    dadosTabelaEmpresasVisualizadas[index].nome = empresaEncontrada.nome;
  })
  //

  const tabelas = {
    0: {
      tabelaEmpresasExportadas: filtrarDadosPorPeriodo(dadosTabelaEmpresasExportadas, 'dt_evento', 7),
      tabelaEmpresasVisualizadas: filtrarDadosPorPeriodo(dadosTabelaEmpresasVisualizadas, 'dt_evento', 7)
    },
    1: {
      tabelaEmpresasExportadas: filtrarDadosPorPeriodo(dadosTabelaEmpresasExportadas, 'dt_evento', 30), 
      tabelaEmpresasVisualizadas: filtrarDadosPorPeriodo(dadosTabelaEmpresasVisualizadas, 'dt_evento', 30)
    },
    2: {
      tabelaEmpresasExportadas: dadosTabelaEmpresasExportadas,
      tabelaEmpresasVisualizadas: dadosTabelaEmpresasVisualizadas
    }
  }
  
  return tabelas;
}

async function buscaNomesEmpresas(cnpjs) {
  const resultado = await axios.post(process.env.API_PATH + "/ecdt-busca/searchNomesByCnpjs", {
    cnpjs: cnpjs
  })

  return resultado.data;
} 

const returnDadosDashboard = async(req , res) =>{
  const { usuarios, cdCliente } = req.body;
  const emailUsuarioEnviouRequest = jwt.decode(req.headers.authorization.split(' ')[1]).user_name;
  
  const dados = await getDadosDashboard(cdCliente, usuarios, 90);

  const dadosConsultaSemanal = await separarDadosPorPeriodo(dados, emailUsuarioEnviouRequest, usuarios, 7);
  const dadosConsultaMensal = await separarDadosPorPeriodo(dados, emailUsuarioEnviouRequest, usuarios, 30);
  const dadosConsultaTrimestral = await separarDadosPorPeriodo(dados, emailUsuarioEnviouRequest, usuarios, 90);

  let resultado = {
    semanal : dadosConsultaSemanal, 
    mensal : dadosConsultaMensal, 
    trimestral: dadosConsultaTrimestral
  };

  return res.status(200).json(resultado);
}

async function  getDadosDashboard (cd_cliente, usuario, periodo) {
  ResultEmail = await platAdminAccess.emailEnviadosValidacaoCliente(cd_cliente, usuario, periodo);
  ResultTelefone = await platAdminAccess.telEnviadosValidacaoCliente(cd_cliente, usuario, periodo);
  ResultPesquisas = await platAdminAccess.pesquisasCliente(cd_cliente, usuario, periodo);
  ResultVisualizacoes = await platAdminAccess.detalheCliente(cd_cliente, usuario, periodo);
  ResultExportacao = await platAdminAccess.exportacaoCliente(cd_cliente, usuario, periodo) 

  return {  
    resultEmail : ResultEmail, 
    resultTelefone : ResultTelefone,
    resultPesquisas : ResultPesquisas,
    resultVisualizacoes : ResultVisualizacoes,
    resultExportacao : ResultExportacao 
  };
}

async function separarDadosPorPeriodo(dados, emailUsuarioEnviouRequest, usuario, periodo) {
  const response = {  
    resultEmail : filtrarDadosPorPeriodo(dados.resultEmail, 'dt_evento', periodo), 
    resultEmailSuccess : filtrarDadosPorPeriodo(dados.resultEmail, 'dt_evento', periodo).filter((evento) => evento.status == 'SUCCESS'),
    resultTelefone : filtrarDadosPorPeriodo(dados.resultTelefone, 'date_event', periodo),
    resultTelefoneSuccess : filtrarDadosPorPeriodo(dados.resultTelefone, 'date_event', periodo).filter((evento) => ['completed', 'busy', 'no-answer'].includes(evento.status_validation)),
    resultPesquisas : filtrarDadosPorPeriodo(dados.resultPesquisas, 'dt_criacao', periodo),
    resultVisualizacoes : filtrarDadosPorPeriodo(dados.resultVisualizacoes, 'dt_evento', periodo),
    resultExportacao : filtrarDadosPorPeriodo(dados.resultExportacao, 'dt_evento', periodo)
   };

   let totalEmailsEnviados = response.resultEmail.length;
   let totalEmailsEnviadosSuccess = response.resultEmailSuccess.length;
   let totalTelefonesEnviados = response.resultTelefone.length;
   let totalTelefonesEnviadosSuccess = response.resultTelefoneSuccess.length;
   let totalEnviadasEnriquecimento = totalEmailsEnviados + totalTelefonesEnviados;
   let totalEnriquecidas = totalEmailsEnviadosSuccess + totalTelefonesEnviadosSuccess;
   let totalPesquisasRealizadas = response.resultPesquisas.length;
   let totalVisualizacoes = response.resultVisualizacoes.length;
   let totalExportacoes = 0;

   for(var i= 0; i < response.resultExportacao.length; i++){
      totalExportacoes += parseInt(response.resultExportacao[i].qtd_empresas);
   }

   let amostraPesquisas = response.resultPesquisas.sort(ordenaLista("dt_criacao"));
   amostraPesquisas = amostraPesquisas.slice(0 , 5);
   let amostraVizualizacoes = response.resultVisualizacoes.sort(ordenaLista("dt_criacao"));
   amostraVizualizacoes = amostraVizualizacoes.slice(0 , 5);
 
  // - Amostra pesquisas apenas do usuário que enviou a request:
  let amostraPesquisasDoUsuario = [];
  if (['Todos os Usuários', emailUsuarioEnviouRequest].includes(usuario)) {
    const pesquisasUsuario = response.resultPesquisas.filter((pesquisa) => pesquisa.email == emailUsuarioEnviouRequest);
    amostraPesquisasDoUsuario = pesquisasUsuario.sort(ordenaLista('dt_criacao'));
    amostraPesquisasDoUsuario = amostraPesquisasDoUsuario.slice(0 , 5); 
  }
  //

    var auxEmail = new Map();
   try {
      for(var i=0; i<response.resultEmail.length; i++){
        let count = 1;
        let day = response.resultEmail[i].dt_evento.toGMTString().split(',')[1].trim().split(' ')[0];
        let month = response.resultEmail[i].dt_evento.toGMTString().split(',')[1].trim().split(' ')[1]
        let year = response.resultEmail[i].dt_evento.toGMTString().split(',')[1].trim().split(' ')[2]

        let date = day + "-" + month + "-" + year ;
        

        if(i > 0){
          if(auxEmail.get(date)){
            let countInterator =  (parseInt(auxEmail.get(date)) + 1);
            auxEmail.set(date, countInterator);
          } else auxEmail.set(date, count);
        }else auxEmail.set(date, count);
        
      }
    
   } catch (error) {
     console.log('error')
   }
  var auxEmailSuccess = new Map();
   try {

    for(var i=0; i<response.resultEmailSuccess.length; i++){
      let count = 1;
      let day = response.resultEmailSuccess[i].dt_evento.toGMTString().split(',')[1].trim().split(' ')[0];
      let month = response.resultEmailSuccess[i].dt_evento.toGMTString().split(',')[1].trim().split(' ')[1]
      let year = response.resultEmailSuccess[i].dt_evento.toGMTString().split(',')[1].trim().split(' ')[2]

      let date = day + "-" + month + "-" + year ;
      

      if(i > 0){
        if(auxEmailSuccess.get(date)){
          let countInterator =  (parseInt(auxEmailSuccess.get(date)) + 1);
          auxEmailSuccess.set(date, countInterator);
        } else auxEmailSuccess.set(date, count);
      }else auxEmailSuccess.set(date, count);
      
    }
 } catch (error) {
   console.log('error')
 }
 var auxTelefone = new Map();
   try {

    for(var i=0; i<response.resultTelefone.length; i++){
      let count = 1;
      let day = response.resultTelefone[i].date_event.toGMTString().split(',')[1].trim().split(' ')[0];
      let month = response.resultTelefone[i].date_event.toGMTString().split(',')[1].trim().split(' ')[1]
      let year = response.resultTelefone[i].date_event.toGMTString().split(',')[1].trim().split(' ')[2]

      let date = day + "-" + month + "-" + year ;
      

      if(i > 0){
        if(auxTelefone.get(date)){
          let countInterator =  (parseInt(auxTelefone.get(date)) + 1);
          auxTelefone.set(date, countInterator);
        } else auxTelefone.set(date, count);
      }else auxTelefone.set(date, count);
      
    }
 } catch (error) {
   console.log('error')
 }
 var auxTelefoneSuccess = new Map();
   try {

    for(var i=0; i<response.resultTelefoneSuccess.length; i++){
      let count = 1;
      let day = response.resultTelefoneSuccess[i].date_event.toGMTString().split(',')[1].trim().split(' ')[0];
      let month = response.resultTelefoneSuccess[i].date_event.toGMTString().split(',')[1].trim().split(' ')[1]
      let year = response.resultTelefoneSuccess[i].date_event.toGMTString().split(',')[1].trim().split(' ')[2]

      let date = day + "-" + month + "-" + year ;
      

      if(i > 0){
        if(auxTelefoneSuccess.get(date)){
          let countInterator =  (parseInt(auxTelefoneSuccess.get(date)) + 1)
          auxTelefoneSuccess.set(date, countInterator);
        } else auxTelefoneSuccess.set(date, count);
      }else auxTelefoneSuccess.set(date, count);
      
    }
 } catch (error) {
   console.log('error')
 }
 var auxPesquisas = new Map();
 try {

  for(var i=0; i<response.resultPesquisas.length; i++){
    let count = 1;
    let day = response.resultPesquisas[i].dt_criacao.toGMTString().split(',')[1].trim().split(' ')[0];
    let month = response.resultPesquisas[i].dt_criacao.toGMTString().split(',')[1].trim().split(' ')[1]
    let year = response.resultPesquisas[i].dt_criacao.toGMTString().split(',')[1].trim().split(' ')[2]

    let date = day + "-" + month + "-" + year ;
    

    if(i > 0){
      if(auxPesquisas.get(date)){
        let countInterator =  (parseInt(auxPesquisas.get(date)) + 1);
        auxPesquisas.set(date, countInterator);
      } else auxPesquisas.set(date, count);
    }else auxPesquisas.set(date, count);
    
  }
} catch (error) {
 console.log('error')
}
var auxVisualizacoes = new Map();
 try {

  for(var i=0; i<response.resultVisualizacoes.length; i++){
    let count = 1;
    let day = response.resultVisualizacoes[i].dt_evento.toGMTString().split(',')[1].trim().split(' ')[0];
    let month = response.resultVisualizacoes[i].dt_evento.toGMTString().split(',')[1].trim().split(' ')[1]
    let year = response.resultVisualizacoes[i].dt_evento.toGMTString().split(',')[1].trim().split(' ')[2]

    let date = day + "-" + month + "-" + year ;
    

    if(i > 0){
      if(auxVisualizacoes.get(date)){
        let countInterator =  (parseInt(auxVisualizacoes.get(date)) + 1);
        auxVisualizacoes.set(date, countInterator);
      } else auxVisualizacoes.set(date, count);
    }else auxVisualizacoes.set(date, count);
    
  }
} catch (error) {
 console.log('error')
}

var auxExportacao = new Map();
 try {

  for(var i=0; i<response.resultExportacao.length; i++){
    let count = parseInt(response.resultExportacao[i].qtd_empresas) ;
    let day = response.resultExportacao[i].dt_evento.toGMTString().split(',')[1].trim().split(' ')[0];
    let month = response.resultExportacao[i].dt_evento.toGMTString().split(',')[1].trim().split(' ')[1]
    let year = response.resultExportacao[i].dt_evento.toGMTString().split(',')[1].trim().split(' ')[2]

    let date = day + "-" + month + "-" + year ;
    

    if(i > 0){
      if(auxExportacao.get(date)){
        let countInterator =  (parseInt(auxExportacao.get(date)) + count);
        auxExportacao.set(date, countInterator);
      } else auxExportacao.set(date, count);
    }else auxExportacao.set(date, count);
    
  }
} catch (error) {
 console.log('error')
}
  
  let retorno = {
    totalEmailsEnviados: totalEmailsEnviados, //total numerico de emails enviados para validação
    enviadasEnriquecimentoEmailDia : Object.fromEntries(auxEmail), // map com quanditade de emails enviados para enriquecimento por dia map(chave = dia , valor = contagem)
    enviadasEnriquecimentoEmailDiaSuccess : Object.fromEntries(auxEmailSuccess), // map com quanditade de emails enriquecidos por dia map(chave = dia , valor = contagem)
    totalEmailsEnviadosSuccess : totalEmailsEnviadosSuccess, //total numerico de emails que foram enriquecidos
    totalTelefonesEnviados : totalTelefonesEnviados, //valor numerico de telefones enviados para validação
    enviadosEnriquecimentoTelefoneDia: Object.fromEntries(auxTelefone), // map contendo quantidades enviadas por dia para validação de telefones map(chave = dia , valor = contagem)
    enviadosEnriquecimentoTelefoneDiaSuccess: Object.fromEntries(auxTelefoneSuccess), // map com a quantidade empresas com telefone enriquecido agrupadas por dias map(chave = dia , valor = contagem)
    totalTelefonesEnviadosSuccess: totalTelefonesEnviadosSuccess, // valor numerico do total de telefones enriquecidos 
    totalEnviadasEnriquecimento : totalEnviadasEnriquecimento, // doma dos valores de telefone + email enviados para enriquecimento
    totalEnriquecidas : totalEnriquecidas, // soma dos totais de telefones e emails enriquecidos no periodo
    totalPesquisasRealizadas: totalPesquisasRealizadas, // valor numerico das pesquisas realizadas no periodo
    pesquisasRealizadasDia : Object.fromEntries(auxPesquisas), 
    totalVisualizacoes: totalVisualizacoes,    // valor numerico do total de visualizações do periodo 
    vizualizacoesRealizadasDia : Object.fromEntries(auxVisualizacoes), //map contendo os valores de visualizacao por dia
    totalExportacoes : totalExportacoes,   //valor numerico total das exportações do periodo 
    exportacoesRealizadasDia : Object.fromEntries(auxExportacao),  // map contendo valores de exportacao agrupados por dia
    ultimasVizualizacoesTabela : amostraVizualizacoes , 
    ultimasPesquisasTabela : amostraPesquisasDoUsuario
  }
    
    return retorno
}

const returnTopFiltrosDashboard = async (req, res) => {
  const { periodo, usuarios, cdCliente } = req.body;
  
  let resultTopFiltros = await platAdminAccess.topFiltros(cdCliente, usuarios, periodo);
  
  let auxTopFiltros = new Map();
  Array.from(resultTopFiltros).forEach(
    element => {
      let filtros = element.payload.split(', ')
      filtros.forEach( filtro =>{
        if(filtro.includes('=')){
            let filtroUnitario = filtro.replace('{').split('=')[0];
            filtroUnitario = filtroUnitario.replace('undefined' , '');
          let count = 1;
          if(auxTopFiltros.has(filtroUnitario)){
            auxTopFiltros.set(filtroUnitario , (parseInt(auxTopFiltros.get(filtroUnitario)) + count));
          } else auxTopFiltros.set(filtroUnitario , count);
        }
      })

    }
  )

  

return res.status(200).json(Object.fromEntries(auxTopFiltros));
}

function ordenaLista(prop) {    
  return function(a, b) {    
      if (a[prop] > b[prop]) {    
          return -1;    
      } else if (a[prop] < b[prop]) {    
          return 1;    
      }    
      return 0;    
  }    
}    

function filtrarDadosPorPeriodo(dados, campo, dias) {
  const dataAtual = new Date();
  const dataLimite = new Date(Date.UTC(dataAtual.getFullYear(), dataAtual.getMonth(), dataAtual.getDate() - dias));
  return dados.filter(item => new Date(item[campo]) >= dataLimite);
}

module.exports = {
  returnDadosDashboard,
  returnTopFiltrosDashboard,
  returnDadosTabelasDashboard,
  procurador

};
