'use strict';

// Behavioral simulation: four real HTTP servers, one Node process, no external dependencies.
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID, createHash } = require('node:crypto');

const fingerprint = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
class Failure extends Error {
  constructor(status, message) { super(message); this.status = status; }
}

async function startLab({ basePort = 8780, dataDir = path.join(__dirname, '.data'), timeoutMs = 300 } = {}) {
  fs.mkdirSync(dataDir, { recursive: true });
  const servers = [], urls = {};
  let active = 0, sequence = Date.now();
  const initial = name => name === 'purchase'
    ? { operations: {}, identities: {}, returned: 0, reservation: null, events: [],
      config: { mode: 'saga', idempotency: true, retryUnknown: true, fault: null } }
    : { receipts: {}, effects: [], balance: 0, quantity: 10, events: [] };
  const states = {};
  function save(name) {
    const file = path.join(dataDir, name + '.json');
    fs.writeFileSync(file + '.tmp', JSON.stringify(states[name], null, 2));
    fs.renameSync(file + '.tmp', file);
  }
  function event(name, message) {
    states[name].events.push({ seq: ++sequence, at: new Date().toISOString(), message });
    states[name].events = states[name].events.slice(-120);
  }
  for (const name of ['purchase', 'invoice', 'people', 'stock']) {
    const file = path.join(dataDir, name + '.json');
    states[name] = fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, 'utf8')) : initial(name);
    // Simplified restart recovery. Production uses owner tokens and an expiring lease.
    if (name === 'purchase') for (const op of Object.values(states.purchase.operations)) {
      if (op.running) {
        op.running = false;
        op.state = Object.values(op.steps).some(s => s.state === 'UNKNOWN') ? 'UNKNOWN' : 'FAILED';
        event(name, 'Reinício: execução interrompida ' + op.id);
      }
    }
    save(name);
  }
  function snapshot() {
    return { urls, timeoutMs, active, ...structuredClone(states) };
  }
  function consumeFault(target) {
    const fault = states.purchase.config.fault;
    if (!fault || fault.target !== target) return null;
    states.purchase.config.fault = null;
    save('purchase');
    return fault.when;
  }
  async function receiver(name, target, key, body) {
    const state = states[name];
    const enabled = states.purchase.config.idempotency && Boolean(key);
    const hash = fingerprint(body);
    // Receipt lookup happens before new business validation (including current stock).
    if (enabled && state.receipts[key]) {
      if (state.receipts[key].hash !== hash) throw new Failure(409, 'Chave reutilizada com outro conteúdo.');
      event(name, target + ': REPLAY, nenhum novo efeito.');
      save(name);
      return state.receipts[key].result;
    }
    const fault = consumeFault(target);
    if (fault === 'before') {
      event(name, target + ': timeout ANTES de aplicar; nenhum efeito.');
      save(name);
      await delay(timeoutMs * 4);
      throw new Failure(503, 'Pedido não foi executado nesta tentativa.');
    }
    if (fault === 'reject') {
      event(name, target + ': rejeição de negócio; nenhum efeito.');
      save(name);
      throw new Failure(422, 'Rejeição de negócio simulada.');
    }
    if (target === 'stock' && state.quantity < body.quantity) {
      event(name, 'Estoque insuficiente; nenhum efeito.'); save(name);
      throw new Failure(422, 'Estoque insuficiente.');
    }
    const result = { id: state.effects.length + 1, target };
    if (target === 'people') state.balance += body.value;
    if (target === 'stock') state.quantity -= body.quantity;
    state.effects.push({ key: key || null, body, result });
    if (enabled) state.receipts[key] = { hash, result };
    event(name, target + ': APLICADO ' + (key || '(sem chave)'));
    // One file commit simulates the transaction of effect + durable receipt.
    save(name);
    if (fault === 'after') {
      event(name, target + ': efeito salvo, mas resposta atrasada além do timeout.'); save(name);
      await delay(timeoutMs * 4);
    }
    return result;
  }

  async function purchase(request) {
    const input = { purchaseId: Number(request.purchaseId), itemId: Number(request.itemId),
      quantity: Number(request.quantity), amountReturned: Number(request.amountReturned ?? 0) };
    if (input.purchaseId !== 123 || input.itemId !== 1 || !Number.isInteger(input.quantity)
        || input.quantity < 1 || !Number.isInteger(input.amountReturned) || input.amountReturned < 0)
      throw new Failure(400, 'Use compra 123, item 1, quantidade inteira positiva e amountReturned >= 0.');
    const state = states.purchase;
    if (state.config.mode === 'original') return purchaseOriginal(input);
    const hash = fingerprint(input); // Deliberately excludes UI metadata.
    let op = state.operations[state.identities[hash]];
    if (!op) {
      if (state.reservation) throw new Failure(409, 'Item reservado por outra operação pendente.');
      op = { id: randomUUID(), input, state: 'PENDING', running: false, steps: {}, invoiceId: null, error: null };
      state.operations[op.id] = op;
      state.identities[hash] = op.id;
      state.reservation = op.id;
      event('purchase', 'Operação criada e item reservado: ' + op.id); save('purchase');
    }
    if (op.state === 'COMPLETED') {
      event('purchase', 'REPLAY da operação concluída: ' + op.id); save('purchase');
      return { operationId: op.id, invoiceId: op.invoiceId, replay: true };
    }
    if (op.running) throw new Failure(409, 'Operação já está em execução.');
    if (state.reservation && state.reservation !== op.id) throw new Failure(409, 'Item pertence a outra operação.');
    op.running = true; op.state = 'RUNNING'; op.error = null; active++;
    save('purchase');
    try {
      if (!op.steps.PREPARE) {
        if (state.returned !== input.amountReturned || state.returned + input.quantity > 10)
          throw new Failure(400, 'Quantidade devolvida mudou ou excede o disponível. Atualize o pedido.');
        state.reservation = op.id;
        op.steps.PREPARE = { state: 'COMPLETED', result: { ...input, value: input.quantity * 10 } };
        event('purchase', 'PREPARE concluído e salvo.'); save('purchase');
      }
      const plan = op.steps.PREPARE.result;
      async function remote(step, target, route, method, payload) {
        const previous = op.steps[step];
        if (previous?.state === 'COMPLETED') {
          event('purchase', step + ': usando resultado salvo.'); save('purchase');
          return previous.result;
        }
        if (previous && !state.config.retryUnknown) throw new Failure(409, step + ': UNKNOWN bloqueado pela configuração.');
        const key = 'purchase:return:1:' + op.id + ':' + step.toLowerCase();
        op.steps[step] = { state: 'UNKNOWN', key, result: null };
        op.currentStep = step;
        event('purchase', step + ': UNKNOWN salvo ANTES do HTTP.'); save('purchase');
        let response;
        try {
          response = await fetch(urls[target] + route, { method,
            headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key },
            body: JSON.stringify(payload), signal: AbortSignal.timeout(timeoutMs) });
        } catch (error) {
          throw new Failure(504, step + ': timeout/erro de comunicação; resultado desconhecido.');
        }
        const result = await response.json();
        if (!response.ok) throw new Failure(response.status, step + ': ' + result.error);
        op.steps[step] = { state: 'COMPLETED', key, result };
        event('purchase', step + ': COMPLETED salvo.'); save('purchase');
        return result;
      }
      const invoice = await remote('INVOICE', 'invoice', '/invoice', 'POST', plan);
      await remote('REFERENCES', 'invoice', '/references', 'POST', { invoiceId: invoice.id, purchaseId: plan.purchaseId });
      await remote('CREDIT:123', 'people', '/suppliers/7/credit/add-credit', 'PUT', { purchaseId: plan.purchaseId, value: plan.value });
      await remote('STOCK', 'stock', '/purchase/return', 'POST', { itemId: plan.itemId, quantity: plan.quantity });
      op.currentStep = 'CONFIRM_LOCAL';
      if (consumeFault('local')) throw new Failure(500, 'Falha local simulada após os efeitos externos.');
      if (state.returned !== plan.amountReturned) throw new Failure(409, 'Compra mudou; reconciliação necessária.');
      state.returned += plan.quantity;
      op.invoiceId = invoice.id; op.state = 'COMPLETED'; state.reservation = null;
      event('purchase', 'CONFIRM_LOCAL: devolução concluída; item liberado.');
      return { operationId: op.id, invoiceId: invoice.id, replay: false };
    } catch (error) {
      op.state = Object.values(op.steps).some(step => step.state === 'UNKNOWN') ? 'UNKNOWN' : 'FAILED';
      op.error = error.message;
      if (!Object.keys(op.steps).length && state.reservation === op.id) state.reservation = null;
      event('purchase', op.state + ': ' + error.message);
      throw error;
    } finally {
      op.running = false; active--; save('purchase');
    }
  }

  async function purchaseOriginal(input) {
    const state = states.purchase;
    // Original purchase lock: serializes calls but cannot undo committed remote effects.
    if (active) throw new Failure(409, 'Fluxo original: outra chamada ainda está em execução.');
    if (state.returned !== input.amountReturned || state.returned + input.quantity > 10)
      throw new Failure(400, 'Quantidade inválida ou alterada. Atualize o pedido.');
    const attempt = { id: randomUUID(), mode: 'original', traceOnly: true, input,
      state: 'RUNNING', running: true, steps: {}, invoiceId: null, error: null };
    // These records are diagnostic traces for the screen, never recovery checkpoints.
    state.operations[attempt.id] = attempt;
    active++;
    event('purchase', 'ORIGINAL: nova tentativa; repetir toda a cadeia, sem chave ou retomada.');
    save('purchase');
    async function remote(step, target, route, method, body) {
      attempt.currentStep = step;
      event('purchase', 'ORIGINAL: enviando ' + step + ' sem Idempotency-Key.'); save('purchase');
      try {
        const response = await fetch(urls[target] + route, { method,
          headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
          signal: AbortSignal.timeout(timeoutMs) });
        const result = await response.json();
        if (!response.ok) throw new Failure(response.status, step + ': ' + result.error);
        attempt.steps[step] = { state: 'COMPLETED', result };
        save('purchase');
        return result;
      } catch (error) {
        attempt.steps[step] = { state: 'FAILED', result: null };
        if (error instanceof Failure) throw error;
        throw new Failure(504, step + ': timeout; efeitos externos podem ter sido aplicados.');
      }
    }
    try {
      const plan = { ...input, value: input.quantity * 10 };
      const invoice = await remote('INVOICE', 'invoice', '/invoice', 'POST', plan);
      await remote('REFERENCES', 'invoice', '/references', 'POST', { invoiceId: invoice.id, purchaseId: input.purchaseId });
      await remote('CREDIT:123', 'people', '/suppliers/7/credit/add-credit', 'PUT', { purchaseId: input.purchaseId, value: plan.value });
      await remote('STOCK', 'stock', '/purchase/return', 'POST', { itemId: input.itemId, quantity: input.quantity });
      if (consumeFault('local')) throw new Failure(500, 'Falha local simulada no fluxo original.');
      // Only the local transaction is committed here; errors leave the local quantity unchanged.
      state.returned += input.quantity;
      attempt.invoiceId = invoice.id; attempt.state = 'COMPLETED';
      event('purchase', 'ORIGINAL: commit local, sem desfazer duplicações externas anteriores.');
      return { attemptId: attempt.id, invoiceId: invoice.id, mode: 'original' };
    } catch (error) {
      attempt.state = 'FAILED'; attempt.error = error.message;
      event('purchase', 'ORIGINAL: rollback LOCAL; nota, crédito e estoque externos permanecem.');
      throw error;
    } finally {
      attempt.running = false; active--; save('purchase');
    }
  }

  async function readBody(req) {
    let text = '';
    for await (const chunk of req) {
      text += chunk;
      if (text.length > 16384) throw new Failure(413, 'Pedido muito grande.');
    }
    try { return JSON.parse(text || '{}'); } catch { throw new Failure(400, 'JSON inválido.'); }
  }
  function json(res, status, body) {
    if (!res.destroyed) { res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' }); res.end(JSON.stringify(body)); }
  }
  async function handle(name, req, res) {
    const route = new URL(req.url, 'http://localhost').pathname;
    if (req.method === 'GET' && route === '/health') return json(res, 200, { service: name });
    if (name === 'purchase') {
      if (req.method === 'GET' && route === '/') {
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        return res.end(fs.readFileSync(path.join(__dirname, 'index.html')));
      }
      if (req.method === 'GET' && route === '/lab/state') return json(res, 200, snapshot());
      if (req.method === 'POST' && route === '/lab/scenario') {
        const { mode } = await readBody(req);
        if (!['original', 'saga'].includes(mode)) throw new Failure(400, 'Cenário inválido.');
        if (active) throw new Failure(409, 'Aguarde a execução antes de preparar o cenário.');
        for (const service of Object.keys(states)) states[service] = initial(service);
        Object.assign(states.purchase.config, { mode, idempotency: mode === 'saga',
          retryUnknown: true, fault: { target: 'stock', when: 'after' } });
        event('purchase', 'Cenário ' + mode + ' preparado: timeout após movimentar estoque.');
        for (const service of Object.keys(states)) save(service);
        return json(res, 200, snapshot());
      }
      if (req.method === 'POST' && route === '/purchases/itens/return') return json(res, 200, await purchase(await readBody(req)));
      if (req.method === 'POST' && route === '/lab/config') {
        const config = await readBody(req);
        if (active) throw new Failure(409, 'Aguarde a execução para alterar a configuração.');
        if (typeof config.idempotency === 'boolean') states.purchase.config.idempotency = config.idempotency;
        if (typeof config.retryUnknown === 'boolean') states.purchase.config.retryUnknown = config.retryUnknown;
        if ('fault' in config) {
          if (config.fault && (!['invoice', 'references', 'people', 'stock', 'local'].includes(config.fault.target)
              || !['before', 'after', 'reject'].includes(config.fault.when))) throw new Failure(400, 'Falha inválida.');
          states.purchase.config.fault = config.fault;
        }
        save('purchase'); return json(res, 200, snapshot());
      }
      if (req.method === 'POST' && route === '/lab/stock') {
        const { quantity } = await readBody(req);
        if (!Number.isInteger(quantity) || quantity < 0) throw new Failure(400, 'Saldo deve ser inteiro e não negativo.');
        states.stock.quantity = quantity; event('stock', 'Outro usuário alterou o saldo para ' + quantity); save('stock');
        return json(res, 200, snapshot());
      }
      if (req.method === 'POST' && route === '/lab/reset') {
        if (active) throw new Failure(409, 'Aguarde a execução antes de reiniciar.');
        for (const service of Object.keys(states)) { states[service] = initial(service); save(service); }
        return json(res, 200, snapshot());
      }
    } else {
      const target = name === 'invoice' ? ({ '/invoice': 'invoice', '/references': 'references' })[route]
        : name === 'people' && route === '/suppliers/7/credit/add-credit' ? 'people'
        : name === 'stock' && route === '/purchase/return' ? 'stock' : null;
      if (target && req.method === (name === 'people' ? 'PUT' : 'POST')) {
        const key = req.headers['idempotency-key'];
        return json(res, 200, await receiver(name, target, key, await readBody(req)));
      }
    }
    throw new Failure(404, 'Rota não encontrada.');
  }
  try {
    for (const [i, name] of ['purchase', 'invoice', 'people', 'stock'].entries()) {
      const server = http.createServer((req, res) => handle(name, req, res)
        .catch(error => json(res, error.status || 500, { error: error.message })));
      servers.push(server);
      await new Promise((resolve, reject) => {
        server.once('error', reject); server.listen(basePort ? basePort + i : 0, '127.0.0.1', resolve);
      });
      urls[name] = 'http://127.0.0.1:' + server.address().port;
    }
  } catch (error) {
    await Promise.all(servers.map(server => new Promise(resolve => server.close(resolve)))); throw error;
  }
  return { urls, snapshot, dataDir, close: async () => {
    await Promise.all(servers.map(server => new Promise(resolve => {
      server.close(resolve); server.closeIdleConnections();
    })));
  } };
}

module.exports = { startLab };
if (require.main === module) {
  startLab({ basePort: Number(process.env.POC_BASE_PORT || 8780) }).then(lab => {
    console.log('POC pronta: ' + lab.urls.purchase);
    console.log(JSON.stringify(lab.urls, null, 2));
    console.log('Dados locais: ' + lab.dataDir + '\nCtrl+C encerra os quatro servidores.');
  }).catch(error => { console.error(error.message); process.exitCode = 1; });
}
