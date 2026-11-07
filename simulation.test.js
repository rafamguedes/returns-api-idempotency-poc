'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { startLab } = require('./server');

const request = { purchaseId: 123, itemId: 1, quantity: 2, amountReturned: 0 };
async function fixture(action) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'purchase-poc-'));
  let lab = await startLab({ basePort: 0, dataDir: dir, timeoutMs: 250 });
  const call = async (route, body = {}, base = lab.urls.purchase, method = 'POST', headers = {}) => {
    const response = await fetch(base + route, { method,
      headers: { 'Content-Type': 'application/json', ...headers }, body: JSON.stringify(body) });
    return { status: response.status, body: await response.json() };
  };
  const send = (body = request) => call('/purchases/itens/return', body);
  const arm = (target, when) => call('/lab/config', { fault: { target, when } });
  try {
    await action({ call, send, arm, state: () => lab.snapshot(), restart: async () => {
      await lab.close(); lab = await startLab({ basePort: 0, dataDir: dir, timeoutMs: 250 });
    } });
  } finally {
    await lab.close();
    // Only remove the exact temporary directory created by this fixture.
    if (!path.resolve(dir).startsWith(path.join(path.resolve(os.tmpdir()), 'purchase-poc-')))
      throw new Error('Unsafe fixture directory');
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

test('caminho feliz e replay sem novos efeitos', () => fixture(async ({ send, state }) => {
  const first = await send(); const replay = await send();
  assert.equal(first.status, 200); assert.equal(replay.status, 200);
  assert.equal(first.body.operationId, replay.body.operationId); assert.equal(replay.body.replay, true);
  assert.equal(state().invoice.effects.length, 2); // One invoice + one reference write.
  assert.equal(state().people.balance, 20); assert.equal(state().stock.quantity, 8);
  assert.equal(state().purchase.returned, 2); assert.equal(state().purchase.reservation, null);
}));

test('timeout após estoque aplicado: replay prevalece mesmo com saldo alterado para zero', () => fixture(async ({ send, arm, call, state }) => {
  await arm('stock', 'after'); assert.equal((await send()).status, 504);
  assert.equal(state().stock.quantity, 8); assert.equal(state().purchase.returned, 0);
  await call('/lab/stock', { quantity: 0 });
  assert.equal((await send()).status, 200);
  assert.equal(state().stock.quantity, 0); assert.equal(state().stock.effects.length, 1);
  assert.equal(state().people.effects.length, 1); assert.equal(state().purchase.returned, 2);
}));

test('timeout antes do efeito: saldo atual pode impedir conclusão; não há compensação', () => fixture(async ({ send, arm, call, state }) => {
  await arm('stock', 'before'); assert.equal((await send()).status, 504);
  assert.equal(state().stock.effects.length, 0);
  await call('/lab/stock', { quantity: 0 }); assert.equal((await send()).status, 422);
  assert.equal(state().people.balance, 20); assert.equal(state().purchase.returned, 0);
  assert.ok(state().purchase.reservation);
  await call('/lab/stock', { quantity: 5 }); assert.equal((await send()).status, 200);
  assert.equal(state().stock.quantity, 3); assert.equal(state().people.effects.length, 1);
}));

test('sem idempotência no receptor: timeout pode duplicar crédito', () => fixture(async ({ send, arm, call, state }) => {
  await call('/lab/config', { idempotency: false });
  await arm('people', 'after'); assert.equal((await send()).status, 504);
  assert.equal((await send()).status, 200);
  assert.equal(state().people.balance, 40); assert.equal(state().people.effects.length, 2);
  assert.equal(state().purchase.returned, 2);
}));

test('proteção de UNKNOWN impede reenvio quando desabilitado', () => fixture(async ({ send, arm, call, state }) => {
  await call('/lab/config', { retryUnknown: false });
  await arm('stock', 'after'); assert.equal((await send()).status, 504);
  assert.equal((await send()).status, 409); assert.equal(state().stock.effects.length, 1);
  await call('/lab/config', { retryUnknown: true }); assert.equal((await send()).status, 200);
}));

test('falha local após todos os efeitos: nova tentativa só confirma na purchase', () => fixture(async ({ send, arm, state }) => {
  await arm('local', 'reject'); assert.equal((await send()).status, 500);
  assert.equal(state().purchase.returned, 0); assert.equal(state().stock.quantity, 8);
  assert.equal((await send()).status, 200);
  assert.equal(state().people.effects.length, 1); assert.equal(state().stock.effects.length, 1);
  assert.equal(state().purchase.returned, 2);
}));

test('mesma chave com outro conteúdo retorna conflito no receptor', () => fixture(async ({ send, call, state }) => {
  await send(); const credit = state().people.effects[0];
  const result = await call('/suppliers/7/credit/add-credit', { ...credit.body, value: 99 },
    state().urls.people, 'PUT', { 'Idempotency-Key': credit.key });
  assert.equal(result.status, 409); assert.equal(state().people.balance, 20);
}));

test('duas chamadas concorrentes assumem uma única operação', () => fixture(async ({ send, state }) => {
  const results = await Promise.all([send(), send()]);
  assert.deepEqual(results.map(r => r.status).sort(), [200, 409]);
  assert.equal(Object.keys(state().purchase.operations).length, 1);
  assert.equal(state().stock.effects.length, 1); assert.equal(state().people.effects.length, 1);
}));

test('pedido alterado não assume item já reservado por operação pendente', () => fixture(async ({ send, arm, state }) => {
  await arm('stock', 'after'); await send();
  assert.equal((await send({ ...request, quantity: 3 })).status, 409);
  assert.equal(Object.keys(state().purchase.operations).length, 1);
  assert.equal((await send()).status, 200);
}));

test('reinício preserva os recibos e o progresso para retomar sem duplicação', () => fixture(async ({ send, arm, restart, state }) => {
  await arm('stock', 'after'); const first = await send(); assert.equal(first.status, 504);
  const id = Object.keys(state().purchase.operations)[0];
  await restart(); const next = await send(); assert.equal(next.status, 200);
  assert.equal(next.body.operationId, id); assert.equal(state().stock.effects.length, 1);
  assert.equal(state().people.balance, 20);
}));

test('metadados da interface não criam outra identidade', () => fixture(async ({ send, state }) => {
  const first = await send(); const next = await send({ ...request, quantity: '2', linked: true });
  assert.equal(next.body.operationId, first.body.operationId);
  assert.equal(state().people.effects.length, 1);
}));

test('preparação rejeitada libera o item para um pedido corrigido', () => fixture(async ({ send, state }) => {
  assert.equal((await send({ ...request, quantity: 11 })).status, 400);
  assert.equal(state().purchase.reservation, null); assert.equal(state().invoice.effects.length, 0);
  assert.equal((await send()).status, 200);
}));

test('fluxo original: timeout no estoque e repetição duplicam toda a cadeia externa', () => fixture(async ({ send, call, state }) => {
  await call('/lab/scenario', { mode: 'original' });
  assert.equal((await send()).status, 504);
  assert.equal(state().purchase.returned, 0); assert.equal(state().people.balance, 20);
  assert.equal(state().stock.quantity, 8);
  const second = await send(); assert.equal(second.status, 200);
  assert.equal(state().invoice.effects.filter(e => e.result.target === 'invoice').length, 2);
  assert.equal(state().invoice.effects.filter(e => e.result.target === 'references').length, 2);
  assert.equal(state().people.balance, 40); assert.equal(state().stock.quantity, 6);
  assert.equal(state().purchase.returned, 2);
  assert.equal(Object.keys(state().purchase.identities).length, 0);
  assert.equal(Object.keys(state().purchase.operations).length, 2);
  assert.ok(state().stock.effects.every(e => e.key === null));
  assert.equal(Object.keys(state().stock.receipts).length, 0);
}));

test('solução atual: o mesmo cenário guiado conclui sem duplicar', () => fixture(async ({ send, call, state }) => {
  await call('/lab/scenario', { mode: 'saga' });
  assert.equal((await send()).status, 504); assert.equal((await send()).status, 200);
  assert.equal(state().invoice.effects.filter(e => e.result.target === 'invoice').length, 1);
  assert.equal(state().people.balance, 20); assert.equal(state().stock.quantity, 8);
  assert.equal(state().purchase.returned, 2); assert.equal(Object.keys(state().purchase.operations).length, 1);
}));

test('fluxo original: timeout no crédito repete a nota e o crédito, mas não o estoque ainda não chamado', () => fixture(async ({ send, arm, call, state }) => {
  await call('/lab/scenario', { mode: 'original' }); await arm('people', 'after');
  assert.equal((await send()).status, 504); assert.equal(state().stock.quantity, 10);
  assert.equal((await send()).status, 200);
  assert.equal(state().invoice.effects.filter(e => e.result.target === 'invoice').length, 2);
  assert.equal(state().people.balance, 40); assert.equal(state().stock.quantity, 8);
}));

test('fluxo original sem chave continua duplicando mesmo se o receptor suporta idempotência', () => fixture(async ({ send, call, state }) => {
  await call('/lab/scenario', { mode: 'original' });
  await call('/lab/config', { idempotency: true });
  assert.equal((await send()).status, 504); assert.equal((await send()).status, 200);
  assert.equal(state().stock.effects.length, 2); assert.equal(state().stock.quantity, 6);
  assert.equal(Object.keys(state().stock.receipts).length, 0);
}));

test('cenário original também serializa chamadas simultâneas sem prometer replay', () => fixture(async ({ send, call, state }) => {
  await call('/lab/scenario', { mode: 'original' }); await call('/lab/config', { fault: null });
  const results = await Promise.all([send(), send()]);
  assert.deepEqual(results.map(r => r.status).sort(), [200, 409]);
  assert.equal(state().people.effects.length, 1);
}));
