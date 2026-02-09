import type { IActionHttp, IActorHttpOutput, IActorHttpArgs, MediatorHttp } from '@comunica/bus-http';
import { ActorHttp } from '@comunica/bus-http';
import { KeysHttp } from '@comunica/context-entries';
import type { TestResult } from '@comunica/core';
import { ActionContextKey, failTest, passTest } from '@comunica/core';
import type { IMediatorTypeTime } from '@comunica/mediatortype-time';
import { PassThrough } from 'readable-stream';

export class ActorHttpRetryBody extends ActorHttp {
  private readonly mediatorHttp: MediatorHttp;

  // Context key to indicate that the actor has already wrapped the given request
  private static readonly keyWrapped = new ActionContextKey<boolean>('urn:comunica:actor-http-retry-body#wrapped');

  public constructor(args: IActorHttpRetryBodyArgs) {
    super(args);
    this.mediatorHttp = args.mediatorHttp;
  }

  public async test(action: IActionHttp): Promise<TestResult<IMediatorTypeTime>> {
    if (action.context.has(ActorHttpRetryBody.keyWrapped)) {
      return failTest(`${this.name} can only wrap a request once`);
    }
    const retryCount = action.context.get(KeysHttp.httpRetryBodyCount);
    if (!retryCount || retryCount < 1) {
      return failTest(`${this.name} requires a retry count greater than zero to function`);
    }
    return passTest({ time: 0 });
  }

  public async run(action: IActionHttp): Promise<IActorHttpOutput> {
    const url = ActorHttp.getInputUrl(action.input);
    const retryCount = action.context.getSafe(KeysHttp.httpRetryBodyCount);
    const retryDelayFallback = action.context.get(KeysHttp.httpRetryBodyDelayFallback) ?? 0;
    const allowUnsafe = action.context.get(KeysHttp.httpRetryBodyAllowUnsafe) ?? false;
    const maxBytes = action.context.get(KeysHttp.httpRetryBodyMaxBytes);

    const response = await this.mediatorHttp.mediate({
      ...action,
      context: action.context.set(ActorHttpRetryBody.keyWrapped, true),
    });

    if (!response.ok || !response.body) {
      return response;
    }

    const method = ActorHttpRetryBody.getRequestMethod(action);
    if (!ActorHttpRetryBody.isIdempotentMethod(method) && !allowUnsafe) {
      this.logWarn(action.context, 'Skipping body retry for non-idempotent request method', () => ({
        url: url.href,
        method,
      }));
      return response;
    }

    if (!ActorHttpRetryBody.isReplayableRequestBody(action) && !allowUnsafe) {
      this.logWarn(action.context, 'Skipping body retry for non-replayable request body', () => ({
        url: url.href,
        method,
      }));
      return response;
    }

    const attemptLimit = retryCount + 1;

    if (maxBytes !== undefined) {
      const contentLengthHeader = response.headers.get('content-length');
      if (contentLengthHeader) {
        const contentLength = Number.parseInt(contentLengthHeader, 10);
        if (Number.isFinite(contentLength) && contentLength > maxBytes) {
          this.logWarn(action.context, 'Skipping body retry due to content-length exceeding max bytes', () => ({
            url: url.href,
            contentLength,
            maxBytes,
          }));
          return response;
        }
      }
    }

    const retryingBody = this.createRetryingBody(
      ActorHttp.toNodeReadable(response.body),
      action,
      attemptLimit,
      retryDelayFallback,
      url,
      maxBytes,
    );

    const retryingBodyWeb = ActorHttp.toWebReadableStream(retryingBody);
    Object.defineProperty(response, 'body', {
      configurable: true,
      enumerable: true,
      get: () => retryingBodyWeb,
    });
    return response;
  }

  /**
   * Create a readable stream that buffers a response body per attempt,
   * and retries the full request when the body stream errors or closes prematurely.
   *
   * When `maxBytes` is exceeded while buffering, retries are disabled and the body
   * switches to pass-through streaming to avoid excessive memory usage.
   * @param {NodeJS.ReadableStream} initialBody The initial response body stream.
   * @param {IActionHttp} action The original HTTP action.
   * @param {number} attemptLimit Maximum number of attempts (initial attempt + retries).
   * @param {number} retryDelayFallback The fallback delay (ms) between retry attempts.
   * @param {URL} url The request URL (for logging).
   * @param {number | undefined} maxBytes Maximum buffer size per attempt.
   */
  private createRetryingBody(
    initialBody: NodeJS.ReadableStream,
    action: IActionHttp,
    attemptLimit: number,
    retryDelayFallback: number,
    url: URL,
    maxBytes: number | undefined,
  ): NodeJS.ReadableStream {
    const output = new PassThrough();
    let attempts = 1;
    let done = false;
    let retrying = false;
    let currentBody: NodeJS.ReadableStream | undefined;
    let bufferedBytes = 0;

    const isOutputClosed = (): boolean => done || output.destroyed;

    const destroyCurrentBody = (): void => {
      if (currentBody && 'destroy' in currentBody && typeof currentBody.destroy === 'function') {
        currentBody.destroy();
      }
    };

    const pipeBody = (body: NodeJS.ReadableStream): void => {
      currentBody = body;
      bufferedBytes = 0;
      const chunks: Buffer[] = [];
      let overflowHandled = false;

      const cleanup = (): void => {
        body.removeListener('data', onData);
        body.removeListener('error', onError);
        body.removeListener('end', onEnd);
        body.removeListener('close', onClose);
      };

      const onData = (chunk: any): void => {
        const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        bufferedBytes += buffer.length;
        chunks.push(buffer);

        if (!overflowHandled && maxBytes !== undefined && bufferedBytes > maxBytes) {
          overflowHandled = true;
          if ('pause' in body && typeof body.pause === 'function') {
            body.pause();
          }
          cleanup();

          this.logWarn(action.context, 'Max bytes exceeded, disabling body retry and switching to streaming', () => ({
            url: url.href,
            maxBytes,
            bufferedBytes,
            currentAttempt: `${attempts} / ${attemptLimit}`,
          }));

          for (const chunk_ of chunks) {
            if (isOutputClosed()) {
              return;
            }
            output.write(chunk_);
          }
          chunks.length = 0;

          let bodyEnded = false;
          body.once('end', () => {
            bodyEnded = true;
          });
          body.once('error', (error: unknown) => {
            output.destroy(error instanceof Error ? error : new Error(String(error)));
          });
          body.once('close', () => {
            if (!bodyEnded && !isOutputClosed()) {
              output.destroy(new Error('Response body closed before end after disabling body retry due to maxBytes'));
            }
          });

          if ('pipe' in body && typeof body.pipe === 'function') {
            body.pipe(output);
          }
        }
      };

      const onEnd = (): void => {
        cleanup();
        if (isOutputClosed()) {
          done = true;
          return;
        }
        for (const chunk of chunks) {
          output.write(chunk);
        }
        chunks.length = 0;
        done = true;
        output.end();
      };

      const onError = (error: unknown): void => {
        cleanup();
        if (isOutputClosed()) {
          done = true;
          return;
        }
        handleStreamError(error).catch((error_) => {
          output.destroy(error_ instanceof Error ? error_ : new Error(String(error_)));
        });
      };

      const onClose = (): void => {
        cleanup();
        if (isOutputClosed()) {
          done = true;
          return;
        }
        handleStreamError(new Error('Response body closed before end during body retry')).catch((error_) => {
          output.destroy(error_ instanceof Error ? error_ : new Error(String(error_)));
        });
      };

      body.on('data', onData);
      body.on('error', onError);
      body.on('end', onEnd);
      body.on('close', onClose);
    };

    const handleStreamError = async(error: unknown): Promise<void> => {
      if (retrying || isOutputClosed()) {
        return;
      }

      const abortSignals = [
        action.init?.signal,
        action.context.get(KeysHttp.httpAbortSignal),
      ];
      if (abortSignals.some(signal => signal?.aborted)) {
        output.destroy(error instanceof Error ? error : new Error(String(error)));
        return;
      }

      if (attempts >= attemptLimit) {
        output.destroy(error instanceof Error ? error : new Error(String(error)));
        return;
      }
      retrying = true;
      attempts++;

      this.logDebug(action.context, 'Retrying response body stream after error', () => ({
        url: url.href,
        bufferedBytes,
        currentAttempt: `${attempts} / ${attemptLimit}`,
      }));

      try {
        destroyCurrentBody();

        if (retryDelayFallback > 0) {
          await ActorHttpRetryBody.sleep(retryDelayFallback);
        }

        if (isOutputClosed()) {
          return;
        }

        const response = await this.mediatorHttp.mediate({
          ...action,
          context: action.context.set(ActorHttpRetryBody.keyWrapped, true),
        });

        if (!response.ok || !response.body) {
          output.destroy(new Error(`Response body retry failed for ${url.href}`));
          return;
        }

        const body = ActorHttp.toNodeReadable(response.body);
        if (isOutputClosed()) {
          if ('destroy' in body && typeof body.destroy === 'function') {
            body.destroy();
          }
          return;
        }
        pipeBody(body);
      } catch (error_: unknown) {
        output.destroy(error_ instanceof Error ? error_ : new Error(String(error_)));
      } finally {
        retrying = false;
      }
    };

    output.on('close', () => {
      done = true;
      destroyCurrentBody();
    });
    output.on('error', () => {
      done = true;
      destroyCurrentBody();
    });

    pipeBody(initialBody);
    return output;
  }

  /**
   * Determine the HTTP method for the given action.
   * Falls back to `GET` if no method can be derived.
   * @param {IActionHttp} action HTTP action.
   */
  public static getRequestMethod(action: IActionHttp): string {
    if (action.init?.method !== undefined) {
      return action.init.method;
    }
    if (action.input instanceof Request) {
      return action.input.method;
    }
    return 'GET';
  }

  /**
   * Check if the given method is idempotent.
   * @param {string} method HTTP method.
   */
  public static isIdempotentMethod(method: string): boolean {
    switch (method.toUpperCase()) {
      case 'GET':
      case 'HEAD':
      case 'PUT':
      case 'DELETE':
      case 'OPTIONS':
      case 'TRACE':
        return true;
      default:
        return false;
    }
  }

  /**
   * Check if the request body can be replayed across retries.
   * @param {IActionHttp} action HTTP action.
   */
  public static isReplayableRequestBody(action: IActionHttp): boolean {
    if (action.init?.body !== undefined) {
      return ActorHttpRetryBody.isReplayableBody(action.init.body);
    }
    if (action.input instanceof Request && action.input.body) {
      return ActorHttpRetryBody.isReplayableBody(action.input.body);
    }
    return true;
  }

  /**
   * Check if a body value is replayable, i.e., it can be sent again without re-reading a stream.
   * @param {unknown} body A request body value.
   */
  public static isReplayableBody(body: unknown): boolean {
    if (
      body === null ||
      body === undefined ||
      typeof body === 'string' ||
      body instanceof URLSearchParams ||
      (typeof FormData !== 'undefined' && body instanceof FormData) ||
      body instanceof ArrayBuffer ||
      ArrayBuffer.isView(body) ||
      (typeof Blob !== 'undefined' && body instanceof Blob)
    ) {
      return true;
    }
    return false;
  }

  /**
   * Sleeps for the specified amount of time, using a timeout
   * @param {number} ms The amount of milliseconds to sleep
   */
  public static async sleep(ms: number): Promise<void> {
    if (ms > 0) {
      await new Promise(resolve => setTimeout(resolve, ms));
    }
  }
}

export interface IActorHttpRetryBodyArgs extends IActorHttpArgs {
  /**
   * The HTTP mediator.
   */
  mediatorHttp: MediatorHttp;
}
