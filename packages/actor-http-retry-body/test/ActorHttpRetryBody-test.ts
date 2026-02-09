import type { IActionHttp, IActorHttpOutput, MediatorHttp } from '@comunica/bus-http';
import { ActorHttp } from '@comunica/bus-http';
import { KeysHttp } from '@comunica/context-entries';
import type { IActorTest } from '@comunica/core';
import { Bus, ActionContext } from '@comunica/core';
import arrayifyStream from 'arrayify-stream';
import { Readable } from 'readable-stream';
import { ActorHttpRetryBody } from '../lib/ActorHttpRetryBody';
import '@comunica/utils-jest';

describe('ActorHttpRetryBody', () => {
  let bus: Bus<ActorHttp, IActionHttp, IActorTest, IActorHttpOutput>;
  let actor: ActorHttpRetryBody;
  let context: ActionContext;
  let mediatorHttp: MediatorHttp;
  let input: string;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
    mediatorHttp = <any> {
      mediate: jest.fn().mockRejectedValue(new Error('mediatorHttp.mediate called without mocking')),
    };
    input = 'http://127.0.0.1/abc';
    actor = new ActorHttpRetryBody({ bus, mediatorHttp, name: 'actor' });
    context = new ActionContext({ [KeysHttp.httpRetryBodyCount.name]: 1 });
    jest.spyOn(<any> actor, 'logDebug').mockImplementation((...args) => (<() => unknown>args[2])());
    jest.spyOn(<any> actor, 'logWarn').mockImplementation((...args) => (<() => unknown>args[2])());
  });

  afterEach(() => {
    jest.resetAllMocks();
    jest.restoreAllMocks();
  });

  describe('test', () => {
    it('should reject without retry count in the context', async() => {
      const context = new ActionContext();
      await expect(actor.test({ input, context })).resolves
        .toFailTest(`${actor.name} requires a retry count greater than zero to function`);
    });

    it('should reject with retry count below 1 in the context', async() => {
      const context = new ActionContext({ [KeysHttp.httpRetryBodyCount.name]: 0 });
      await expect(actor.test({ input, context })).resolves.toFailTest(`${actor.name} requires a retry count greater than zero to function`);
    });

    it('should reject when the action has already been wrapped by it once', async() => {
      const context = new ActionContext({ [(<any>ActorHttpRetryBody).keyWrapped.name]: true });
      await expect(actor.test({ input, context })).resolves
        .toFailTest(`${actor.name} can only wrap a request once`);
    });

    it('should accept when retry count is provided in the context', async() => {
      const context = new ActionContext({ [KeysHttp.httpRetryBodyCount.name]: 1 });
      await expect(actor.test({ input, context })).resolves.toPassTest({ time: 0 });
    });
  });

  describe('run', () => {
    const createErrorBody = (chunk?: string): Readable => new Readable({
      read() {
        if (chunk) {
          this.push(chunk);
        }
        this.destroy(new Error('Body stream error'));
      },
    });

    it('should retry and emit the successful response after stream error', async() => {
      const firstBody = new Readable({
        read() {
          this.push('abc');
          this.destroy(new Error('Body stream error'));
        },
      });
      const secondBody = Readable.from([ 'abcdef' ]);
      const responses: IActorHttpOutput[] = [
        <any> { ok: true, status: 200, body: firstBody, headers: new Headers() },
        <any> { ok: true, status: 200, body: secondBody, headers: new Headers() },
      ];

      jest.spyOn(mediatorHttp, 'mediate')
        .mockResolvedValueOnce(responses.shift()!)
        .mockResolvedValueOnce(responses.shift()!);

      const response = await actor.run({ input, context });
      const chunks = await arrayifyStream(ActorHttp.toNodeReadable(<any> response.body));
      const buffer = Buffer.concat(chunks.map((chunk: any) => Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));
      expect(buffer.toString()).toBe('abcdef');
      expect(mediatorHttp.mediate).toHaveBeenCalledTimes(2);
    });

    it('should retry when body closes without ending', async() => {
      const firstBody = new Readable({
        read() {
          this.push('abc');
          this.destroy();
        },
      });
      const secondBody = Readable.from([ 'abcdef' ]);
      const responses: IActorHttpOutput[] = [
        <any> { ok: true, status: 200, body: firstBody, headers: new Headers() },
        <any> { ok: true, status: 200, body: secondBody, headers: new Headers() },
      ];

      jest.spyOn(mediatorHttp, 'mediate')
        .mockResolvedValueOnce(responses.shift()!)
        .mockResolvedValueOnce(responses.shift()!);

      const response = await actor.run({ input, context });
      const chunks = await arrayifyStream(ActorHttp.toNodeReadable(<any> response.body));
      const buffer = Buffer.concat(chunks.map((chunk: any) => Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));
      expect(buffer.toString()).toBe('abcdef');
      expect(mediatorHttp.mediate).toHaveBeenCalledTimes(2);
    });

    it('should skip retries for non-idempotent methods by default', async() => {
      const body = Readable.from([ 'abc' ]);
      jest.spyOn(mediatorHttp, 'mediate').mockResolvedValue(<any> {
        ok: true,
        status: 200,
        body,
        headers: new Headers(),
      });

      const response = await actor.run({ input, context, init: { method: 'POST' }});
      expect(response.body).toBe(body);
      expect(mediatorHttp.mediate).toHaveBeenCalledTimes(1);
      expect((<any> actor).logWarn).toHaveBeenCalledWith(
        context,
        'Skipping body retry for non-idempotent request method',
        expect.any(Function),
      );
    });

    it('should allow retries for non-idempotent methods when allowed', async() => {
      const context = new ActionContext({
        [KeysHttp.httpRetryBodyCount.name]: 1,
        [KeysHttp.httpRetryBodyAllowUnsafe.name]: true,
      });
      const firstBody = createErrorBody('abc');
      const secondBody = Readable.from([ 'abcdef' ]);
      jest.spyOn(mediatorHttp, 'mediate')
        .mockResolvedValueOnce(<any> { ok: true, status: 200, body: firstBody, headers: new Headers() })
        .mockResolvedValueOnce(<any> { ok: true, status: 200, body: secondBody, headers: new Headers() });

      const response = await actor.run({ input, context, init: { method: 'POST' }});
      const chunks = await arrayifyStream(ActorHttp.toNodeReadable(<any> response.body));
      const buffer = Buffer.concat(chunks.map((chunk: any) => Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));
      expect(buffer.toString()).toBe('abcdef');
      expect(mediatorHttp.mediate).toHaveBeenCalledTimes(2);
    });

    it('should skip retries for non-replayable request bodies', async() => {
      const body = Readable.from([ 'abc' ]);
      const requestBody = new Readable({
        read() {
          this.push('request');
          this.push(null);
        },
      });
      jest.spyOn(mediatorHttp, 'mediate').mockResolvedValue(<any> {
        ok: true,
        status: 200,
        body,
        headers: new Headers(),
      });

      const response = await actor.run({ input, context, init: { body: <any> requestBody }});
      expect(response.body).toBe(body);
      expect(mediatorHttp.mediate).toHaveBeenCalledTimes(1);
      expect((<any> actor).logWarn).toHaveBeenCalledWith(
        context,
        'Skipping body retry for non-replayable request body',
        expect.any(Function),
      );
    });

    it('should allow retries for non-replayable request bodies when allowed', async() => {
      const context = new ActionContext({
        [KeysHttp.httpRetryBodyCount.name]: 1,
        [KeysHttp.httpRetryBodyAllowUnsafe.name]: true,
      });
      const requestBody = new Readable({
        read() {
          this.push('request');
          this.push(null);
        },
      });
      const firstBody = createErrorBody('abc');
      const secondBody = Readable.from([ 'abcdef' ]);
      jest.spyOn(mediatorHttp, 'mediate')
        .mockResolvedValueOnce(<any> { ok: true, status: 200, body: firstBody, headers: new Headers() })
        .mockResolvedValueOnce(<any> { ok: true, status: 200, body: secondBody, headers: new Headers() });

      const response = await actor.run({ input, context, init: { body: <any> requestBody }});
      const chunks = await arrayifyStream(ActorHttp.toNodeReadable(<any> response.body));
      const buffer = Buffer.concat(chunks.map((chunk: any) => Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));
      expect(buffer.toString()).toBe('abcdef');
      expect(mediatorHttp.mediate).toHaveBeenCalledTimes(2);
    });

    it('should skip wrapping when content-length exceeds maxBytes', async() => {
      const context = new ActionContext({
        [KeysHttp.httpRetryBodyCount.name]: 1,
        [KeysHttp.httpRetryBodyMaxBytes.name]: 2,
      });
      const body = Readable.from([ 'abcdef' ]);
      jest.spyOn(mediatorHttp, 'mediate').mockResolvedValue(<any> {
        ok: true,
        status: 200,
        body,
        headers: new Headers({ 'content-length': '6' }),
      });

      const response = await actor.run({ input, context });
      expect(response.body).toBe(body);
      expect(mediatorHttp.mediate).toHaveBeenCalledTimes(1);
    });

    it('should switch to streaming when maxBytes is exceeded', async() => {
      const context = new ActionContext({
        [KeysHttp.httpRetryBodyCount.name]: 1,
        [KeysHttp.httpRetryBodyMaxBytes.name]: 2,
      });
      const body = Readable.from([ 'abcdef' ]);
      jest.spyOn(mediatorHttp, 'mediate').mockResolvedValue(<any> {
        ok: true,
        status: 200,
        body,
        headers: new Headers(),
      });

      const response = await actor.run({ input, context });
      const chunks = await arrayifyStream(ActorHttp.toNodeReadable(<any> response.body));
      const buffer = Buffer.concat(chunks.map((chunk: any) => Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));
      expect(buffer.toString()).toBe('abcdef');
      expect(mediatorHttp.mediate).toHaveBeenCalledTimes(1);
    });

    it('should not retry when a request init abort signal is already aborted', async() => {
      const abortController = new AbortController();
      abortController.abort();
      const body = createErrorBody('abc');
      jest.spyOn(mediatorHttp, 'mediate').mockResolvedValue(<any> {
        ok: true,
        status: 200,
        body,
        headers: new Headers(),
      });

      const response = await actor.run({ input, context, init: { signal: abortController.signal }});
      await expect(arrayifyStream(ActorHttp.toNodeReadable(<any> response.body)))
        .rejects.toThrow('Body stream error');
      expect(mediatorHttp.mediate).toHaveBeenCalledTimes(1);
    });

    it('should not retry when a context abort signal is already aborted', async() => {
      const abortController = new AbortController();
      abortController.abort();
      const context = new ActionContext({
        [KeysHttp.httpRetryBodyCount.name]: 1,
        [KeysHttp.httpAbortSignal.name]: abortController.signal,
      });
      const body = createErrorBody('abc');
      jest.spyOn(mediatorHttp, 'mediate').mockResolvedValue(<any> {
        ok: true,
        status: 200,
        body,
        headers: new Headers(),
      });

      const response = await actor.run({ input, context });
      await expect(arrayifyStream(ActorHttp.toNodeReadable(<any> response.body)))
        .rejects.toThrow('Body stream error');
      expect(mediatorHttp.mediate).toHaveBeenCalledTimes(1);
    });

    it('should not retry after switching to streaming due to maxBytes', async() => {
      const context = new ActionContext({
        [KeysHttp.httpRetryBodyCount.name]: 1,
        [KeysHttp.httpRetryBodyMaxBytes.name]: 2,
      });
      let pushed = false;
      const body = new Readable({
        read() {
          if (pushed) {
            return;
          }
          pushed = true;
          this.push('abc');
          process.nextTick(() => this.destroy(new Error('Body stream error after overflow')));
        },
      });
      jest.spyOn(mediatorHttp, 'mediate').mockResolvedValue(<any> {
        ok: true,
        status: 200,
        body,
        headers: new Headers(),
      });

      const response = await actor.run({ input, context });
      await expect(arrayifyStream(ActorHttp.toNodeReadable(<any> response.body)))
        .rejects.toThrow('Body stream error after overflow');
      expect(mediatorHttp.mediate).toHaveBeenCalledTimes(1);
    });

    it('should error when retry limit is exhausted', async() => {
      const firstBody = createErrorBody('abc');
      const secondBody = createErrorBody('abc');
      jest.spyOn(mediatorHttp, 'mediate')
        .mockResolvedValueOnce(<any> { ok: true, status: 200, body: firstBody, headers: new Headers() })
        .mockResolvedValueOnce(<any> { ok: true, status: 200, body: secondBody, headers: new Headers() });

      const response = await actor.run({ input, context });
      await expect(arrayifyStream(ActorHttp.toNodeReadable(<any> response.body))).rejects.toThrow('Body stream error');
      expect(mediatorHttp.mediate).toHaveBeenCalledTimes(2);
    });

    it('should discard partial output before retrying', async() => {
      const firstBody = createErrorBody('abcdef');
      const secondBody = Readable.from([ 'abc' ]);
      jest.spyOn(mediatorHttp, 'mediate')
        .mockResolvedValueOnce(<any> { ok: true, status: 200, body: firstBody, headers: new Headers() })
        .mockResolvedValueOnce(<any> { ok: true, status: 200, body: secondBody, headers: new Headers() });

      const response = await actor.run({ input, context });
      const chunks = await arrayifyStream(ActorHttp.toNodeReadable(<any> response.body));
      const buffer = Buffer.concat(chunks.map((chunk: any) => Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));
      expect(buffer.toString()).toBe('abc');
      expect(mediatorHttp.mediate).toHaveBeenCalledTimes(2);
    });

    it('should use retry delay fallback between body retries', async() => {
      const context = new ActionContext({
        [KeysHttp.httpRetryBodyCount.name]: 1,
        [KeysHttp.httpRetryBodyDelayFallback.name]: 123,
      });
      const sleepSpy = jest.spyOn(ActorHttpRetryBody, 'sleep').mockResolvedValue();
      const firstBody = createErrorBody('abc');
      const secondBody = Readable.from([ 'abcdef' ]);
      jest.spyOn(mediatorHttp, 'mediate')
        .mockResolvedValueOnce(<any> { ok: true, status: 200, body: firstBody, headers: new Headers() })
        .mockResolvedValueOnce(<any> { ok: true, status: 200, body: secondBody, headers: new Headers() });

      const response = await actor.run({ input, context });
      await arrayifyStream(ActorHttp.toNodeReadable(<any> response.body));
      expect(sleepSpy).toHaveBeenCalledWith(123);
    });

    it('should be able to wrap native Response instances', async() => {
      jest.spyOn(mediatorHttp, 'mediate').mockResolvedValue(<any> new Response('abcdef'));

      const response = await actor.run({ input, context });
      const chunks = await arrayifyStream(ActorHttp.toNodeReadable(<any> response.body));
      const buffer = Buffer.concat(chunks.map((chunk: any) => Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));
      expect(buffer.toString()).toBe('abcdef');
    });

    it('should pass through when response is not ok', async() => {
      const body = Readable.from([ 'abc' ]);
      jest.spyOn(mediatorHttp, 'mediate').mockResolvedValue(<any> {
        ok: false,
        status: 500,
        body,
        headers: new Headers(),
      });

      const response = await actor.run({ input, context });
      expect(response.body).toBe(body);
      expect(mediatorHttp.mediate).toHaveBeenCalledTimes(1);
    });

    it('should pass through when response has no body', async() => {
      jest.spyOn(mediatorHttp, 'mediate').mockResolvedValue(<any> {
        ok: true,
        status: 200,
        body: null,
        headers: new Headers(),
      });

      const response = await actor.run({ input, context });
      expect(response.body).toBeNull();
      expect(mediatorHttp.mediate).toHaveBeenCalledTimes(1);
    });
  });
});
