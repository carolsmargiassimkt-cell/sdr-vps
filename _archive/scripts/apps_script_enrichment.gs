/**
 * PROJETO: Cacador de Leads Inteligente (manddigital)
 * FUNCAO: Enriquecer empresas do Pipedrive com email, telefone e LinkedIn de decisores
 *
 * Principais ajustes:
 * - pagina por todas as organizations, em vez de olhar so as ultimas 100
 * - evita repetir empresa por id e por nome normalizado
 * - prioriza empresas ainda sem enriquecimento util
 * - busca pessoa-chave no LinkedIn com cargo alvo
 * - cria/atualiza pessoa no Pipedrive quando encontra telefone ou LinkedIn
 */

const API_TOKEN = '5933ebf221d4ff6debe9fc0faeaade62033a2bd0';
const DOMAIN = 'manddigital';
const LIMITE_DIARIO = 20;
const PAGE_SIZE = 100;
const CARGOS_ALVO = [
  'gerente de marketing',
  'head de marketing',
  'coordenador de marketing',
  'diretor de marketing',
  'gerente comercial',
  'head comercial',
  'gerente de vendas',
  'diretor comercial',
  'diretor de vendas',
  'head de vendas',
];

function onOpen() {
  SpreadsheetApp.getUi().createMenu('Robo Mand')
    .addItem('Rodar Enriquecimento Agora', 'executarEnriquecimentoControlado')
    .addSeparator()
    .addItem('Resetar Contador Diario', 'resetarContadorManualmente')
    .addItem('Limpar Memoria de Empresas', 'limparMemoriaEmpresas')
    .addToUi();
}

function executarEnriquecimentoControlado() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const aba = ss.getActiveSheet();
  const props = PropertiesService.getScriptProperties();
  const agora = new Date();
  const hoje = Utilities.formatDate(agora, Session.getScriptTimeZone(), 'yyyy-MM-dd');

  let contagemHoje = parseInt(props.getProperty('CONTAGEM_DIARIA') || '0', 10);
  if (props.getProperty('DATA_EXECUCAO') !== hoje) {
    contagemHoje = 0;
    props.setProperty('DATA_EXECUCAO', hoje);
    props.setProperty('CONTAGEM_DIARIA', '0');
  }

  if (contagemHoje >= LIMITE_DIARIO) {
    SpreadsheetApp.getUi().alert('Limite diario atingido.');
    return;
  }

  garantirCabecalho(aba);

  const empresas = listarOrganizationsPendentes();
  if (!empresas.length) {
    SpreadsheetApp.getUi().alert('Nenhuma empresa pendente para enriquecer.');
    return;
  }

  for (const empresa of empresas) {
    if (contagemHoje >= LIMITE_DIARIO) {
      break;
    }

    const orgKey = `FEITO_${empresa.id}`;
    const orgNameKey = `ORG_${normalizarEmpresa(empresa.name)}`;
    if (props.getProperty(orgKey) || props.getProperty(orgNameKey)) {
      continue;
    }

    const dealId = buscarNegocioAberto(empresa.id);
    const pessoaExistente = buscarPessoaUtilNoCRM(empresa.id);
    let houveEnriquecimento = false;

    if (pessoaExistente.email) {
      registrarLinha(aba, empresa.name, 'EMAIL_CRM', pessoaExistente.email, agora, pessoaExistente.nome, '', dealId);
      houveEnriquecimento = true;
    }

    const telefones = buscarTelefonesNoBing(empresa.name);
    for (const tel of telefones) {
      if (telefoneJaExisteNoCRM(tel)) {
        continue;
      }
      const personId = criarPessoaContato({
        nome: `Contato ${empresa.name}`,
        telefone: tel,
        orgId: empresa.id,
        orgName: empresa.name,
      });
      if (!personId) {
        continue;
      }
      adicionarNotaContato(
        personId,
        `Contato enriquecido automaticamente. Telefone encontrado para ${empresa.name}: ${tel}`,
        empresa.id,
        dealId
      );
      if (dealId) {
        vincularAoNegocio(dealId, personId);
      }
      registrarLinha(aba, empresa.name, 'TELEFONE_BING', tel, agora, `Contato ${empresa.name}`, '', dealId);
      houveEnriquecimento = true;
    }

    const decisor = buscarPessoaChaveLinkedin(empresa.name);
    if (decisor) {
      let personId = buscarPessoaPorLinkedinOuNome(empresa.id, decisor.linkedin, decisor.nome);
      if (!personId) {
        personId = criarPessoaContato({
          nome: decisor.nome || `Decisor ${empresa.name}`,
          orgId: empresa.id,
          orgName: empresa.name,
          cargo: decisor.cargo,
        });
      }
      if (personId) {
        adicionarNotaContato(
          personId,
          montarNotaLinkedin(decisor, empresa.name),
          empresa.id,
          dealId
        );
        if (dealId) {
          vincularAoNegocio(dealId, personId);
        }
      }
      registrarLinha(aba, empresa.name, 'LINKEDIN_DECISOR', decisor.linkedin, agora, decisor.nome, decisor.cargo, dealId);
      houveEnriquecimento = true;
    }

    if (!houveEnriquecimento) {
      registrarLinha(aba, empresa.name, 'SEM_RESULTADO', '-', agora, '', '', dealId);
    } else {
      contagemHoje++;
      props.setProperty('CONTAGEM_DIARIA', String(contagemHoje));
    }

    props.setProperty(orgKey, 'true');
    props.setProperty(orgNameKey, String(empresa.id));
    Utilities.sleep(2500);
  }

  SpreadsheetApp.getUi().alert('Enriquecimento finalizado.');
}

function listarOrganizationsPendentes() {
  const todas = [];
  let start = 0;
  while (true) {
    const url = `https://${DOMAIN}.pipedrive.com/api/v1/organizations?limit=${PAGE_SIZE}&start=${start}&sort=add_time DESC&api_token=${API_TOKEN}`;
    const response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
    const parsed = JSON.parse(response.getContentText());
    if (!parsed.success) {
      throw new Error(`Erro Pipedrive organizations: ${response.getContentText()}`);
    }

    const items = parsed.data || [];
    if (!items.length) {
      break;
    }
    todas.push.apply(todas, items);

    const pagination = parsed.additional_data && parsed.additional_data.pagination;
    if (!pagination || !pagination.more_items_in_collection) {
      break;
    }
    start = pagination.next_start;
  }

  const vistos = {};
  return todas.filter((empresa) => {
    const nome = normalizarEmpresa(empresa.name);
    if (!nome || vistos[nome]) {
      return false;
    }
    vistos[nome] = true;
    const resumo = buscarPessoaUtilNoCRM(empresa.id);
    return !resumo.temContatoUtil;
  });
}

function buscarPessoaUtilNoCRM(orgId) {
  const url = `https://${DOMAIN}.pipedrive.com/api/v1/persons?org_id=${orgId}&limit=100&api_token=${API_TOKEN}`;
  const vazio = { temContatoUtil: false, email: '', nome: '' };

  try {
    const res = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
    const parsed = JSON.parse(res.getContentText());
    const pessoas = parsed.data || [];

    for (const pessoa of pessoas) {
      const emails = pessoa.email || [];
      const phones = pessoa.phone || [];
      const temEmail = emails.some((item) => item && item.value);
      const temPhone = phones.some((item) => item && item.value);
      const notaLinkedin = pessoa.visible_to || '';
      if (temEmail || temPhone || notaLinkedin) {
        return {
          temContatoUtil: true,
          email: temEmail ? emails[0].value : '',
          nome: pessoa.name || '',
        };
      }
    }
  } catch (e) {
    Logger.log(e);
  }

  return vazio;
}

function buscarNegocioAberto(orgId) {
  const url = `https://${DOMAIN}.pipedrive.com/api/v1/organizations/${orgId}/deals?status=open&api_token=${API_TOKEN}`;
  try {
    const res = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
    const data = JSON.parse(res.getContentText()).data;
    return data && data.length > 0 ? data[0].id : null;
  } catch (e) {
    return null;
  }
}

function vincularAoNegocio(dealId, personId) {
  const url = `https://${DOMAIN}.pipedrive.com/api/v1/deals/${dealId}/participants?api_token=${API_TOKEN}`;
  UrlFetchApp.fetch(url, {
    method: 'post',
    contentType: 'application/json',
    muteHttpExceptions: true,
    payload: JSON.stringify({ person_id: personId }),
  });
}

function telefoneJaExisteNoCRM(tel) {
  const telLimpo = limparTelefone(tel);
  if (!telLimpo) {
    return true;
  }

  const url = `https://${DOMAIN}.pipedrive.com/api/v1/persons/search?term=${encodeURIComponent(telLimpo)}&fields=phone&api_token=${API_TOKEN}`;
  const res = JSON.parse(UrlFetchApp.fetch(url, { muteHttpExceptions: true }).getContentText());
  return !!(res.data && res.data.items && res.data.items.length > 0);
}

function criarPessoaContato({ nome, telefone, orgId, orgName, cargo }) {
  const url = `https://${DOMAIN}.pipedrive.com/api/v1/persons?api_token=${API_TOKEN}`;
  const payload = {
    name: nome || `Contato ${orgName}`,
    org_id: orgId,
  };

  if (telefone) {
    payload.phone = [{ value: telefone, primary: true, label: 'work' }];
  }
  if (cargo) {
    payload.label = cargo.substring(0, 50);
  }

  const res = UrlFetchApp.fetch(url, {
    method: 'post',
    contentType: 'application/json',
    muteHttpExceptions: true,
    payload: JSON.stringify(payload),
  });

  const parsed = JSON.parse(res.getContentText());
  return parsed.success && parsed.data ? parsed.data.id : null;
}

function adicionarNotaContato(personId, content, orgId, dealId) {
  const url = `https://${DOMAIN}.pipedrive.com/api/v1/notes?api_token=${API_TOKEN}`;
  const payload = {
    content: content,
    person_id: personId,
    pinned_to_person_flag: 1,
  };
  if (orgId) {
    payload.org_id = orgId;
  }
  if (dealId) {
    payload.deal_id = dealId;
  }

  UrlFetchApp.fetch(url, {
    method: 'post',
    contentType: 'application/json',
    muteHttpExceptions: true,
    payload: JSON.stringify(payload),
  });
}

function buscarTelefonesNoBing(empresa) {
  const query = encodeURIComponent(`telefone contato empresa ${empresa}`);
  const url = `https://www.bing.com/search?q=${query}`;
  try {
    const res = UrlFetchApp.fetch(url, {
      muteHttpExceptions: true,
      headers: { 'User-Agent': 'Mozilla/5.0' },
    });
    const html = res.getContentText();
    const regex = /\(?\d{2}\)?\s?\d{4,5}[-\s]?\d{4}/g;
    const matches = html.match(regex) || [];
    const limpos = matches
      .map((t) => limparTelefone(t))
      .filter((t) => t.length >= 10 && t.length <= 13);
    return Array.from(new Set(limpos)).slice(0, 3);
  } catch (e) {
    Logger.log(e);
    return [];
  }
}

function buscarPessoaChaveLinkedin(empresa) {
  for (const cargo of CARGOS_ALVO) {
    const query = encodeURIComponent(`site:linkedin.com/in "${empresa}" "${cargo}"`);
    const url = `https://www.bing.com/search?q=${query}`;
    try {
      const res = UrlFetchApp.fetch(url, {
        muteHttpExceptions: true,
        headers: { 'User-Agent': 'Mozilla/5.0' },
      });
      const html = res.getContentText();
      const resultado = extrairPrimeiroLinkedinComContexto(html, cargo);
      if (resultado) {
        return resultado;
      }
    } catch (e) {
      Logger.log(e);
    }
    Utilities.sleep(800);
  }
  return null;
}

function extrairPrimeiroLinkedinComContexto(html, cargoBuscado) {
  const blocks = html.match(/<li class="b_algo".*?<\/li>/gs) || [];
  for (const block of blocks) {
    const urlMatch = block.match(/https:\/\/www\.linkedin\.com\/in\/[a-zA-Z0-9\-_/%]+/);
    if (!urlMatch) {
      continue;
    }
    const titulo = limparHtml(block.match(/<h2.*?>(.*?)<\/h2>/i)?.[1] || '');
    const snippet = limparHtml(block.match(/<p>(.*?)<\/p>/i)?.[1] || '');
    const contexto = `${titulo} ${snippet}`.toLowerCase();
    if (cargoBuscado && contexto.indexOf(cargoBuscado.toLowerCase()) === -1) {
      continue;
    }
    const nome = extrairNomeDoTitulo(titulo, urlMatch[0]);
    return {
      nome: nome,
      cargo: cargoBuscado,
      linkedin: urlMatch[0],
      titulo: titulo,
      snippet: snippet,
    };
  }
  return null;
}

function buscarPessoaPorLinkedinOuNome(orgId, linkedin, nome) {
  const url = `https://${DOMAIN}.pipedrive.com/api/v1/persons?org_id=${orgId}&limit=100&api_token=${API_TOKEN}`;
  try {
    const res = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
    const pessoas = JSON.parse(res.getContentText()).data || [];
    const nomeNorm = normalizarEmpresa(nome || '');
    const linkedinNorm = String(linkedin || '').toLowerCase().trim();

    for (const pessoa of pessoas) {
      const personName = normalizarEmpresa(pessoa.name || '');
      if (nomeNorm && personName === nomeNorm) {
        return pessoa.id;
      }
      const notesUrl = `https://${DOMAIN}.pipedrive.com/api/v1/notes?person_id=${pessoa.id}&api_token=${API_TOKEN}`;
      const notesRes = UrlFetchApp.fetch(notesUrl, { muteHttpExceptions: true });
      const notes = JSON.parse(notesRes.getContentText()).data || [];
      if (linkedinNorm && notes.some((note) => String(note.content || '').toLowerCase().indexOf(linkedinNorm) >= 0)) {
        return pessoa.id;
      }
    }
  } catch (e) {
    Logger.log(e);
  }
  return null;
}

function montarNotaLinkedin(decisor, empresa) {
  return [
    'Contato enriquecido automaticamente via busca publica.',
    `Empresa: ${empresa}`,
    `Nome: ${decisor.nome || '-'}`,
    `Cargo alvo: ${decisor.cargo || '-'}`,
    `LinkedIn: ${decisor.linkedin || '-'}`,
    decisor.snippet ? `Contexto: ${decisor.snippet}` : '',
  ].filter(Boolean).join('<br>');
}

function registrarLinha(aba, empresa, origem, valor, dataHora, nomeContato, cargo, dealId) {
  aba.appendRow([
    empresa || '',
    origem || '',
    valor || '',
    dataHora || new Date(),
    nomeContato || '',
    cargo || '',
    dealId || '',
  ]);
}

function garantirCabecalho(aba) {
  if (aba.getLastRow() > 0) {
    return;
  }
  aba.appendRow([
    'Empresa',
    'Origem',
    'Valor',
    'DataHora',
    'Contato',
    'Cargo',
    'DealID',
  ]);
}

function normalizarEmpresa(valor) {
  return String(valor || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function limparTelefone(valor) {
  return String(valor || '').replace(/\D/g, '');
}

function limparHtml(texto) {
  return String(texto || '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&#39;/g, "'")
    .replace(/&quot;/g, '"')
    .replace(/\s+/g, ' ')
    .trim();
}

function extrairNomeDoTitulo(titulo, linkedinUrl) {
  const texto = String(titulo || '').split('|')[0].split('-')[0].trim();
  if (texto) {
    return texto;
  }
  const slug = String(linkedinUrl || '').split('/in/')[1] || '';
  return slug.split(/[/?#]/)[0].replace(/[-_]/g, ' ').trim();
}

function resetarContadorManualmente() {
  PropertiesService.getScriptProperties().setProperty('CONTAGEM_DIARIA', '0');
  SpreadsheetApp.getUi().alert('Contador resetado!');
}

function limparMemoriaEmpresas() {
  const scriptProps = PropertiesService.getScriptProperties();
  scriptProps.getKeys().forEach((key) => {
    if (key.indexOf('FEITO_') === 0 || key.indexOf('ORG_') === 0) {
      scriptProps.deleteProperty(key);
    }
  });
  SpreadsheetApp.getUi().alert('Memoria limpa!');
}
