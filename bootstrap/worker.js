const REDIS = require('ioredis');
const commands = require('redis-commands');

class Redis {
  constructor(dbo) {
    this.dbo = dbo;
    this.transacted = false;
    this.stacks = [];
  }

  begin() {
    this.transacted = true;
  }

  _exec(cmd) {
    if (cmd && this.transacted && commands.hasFlag(cmd, 'write')) {
      if (typeof this.dbo[cmd] === 'function') {
        return (...args) => this.stacks.push({ cmd, args });
      } else {
        throw new Error('unknow redis command: ' + cmd);
      }
    }
    if (this.dbo[cmd]) {
      return this.dbo[cmd].bind(this.dbo);
    }
  }

  async commit() {
    if (!this.transacted) return;
    const stacks = this.stacks.map(stack => new Promise((resolve, reject) => {
      stack.args.push((err, result) => {
        if (err) return reject(err);
        resolve(result);
      });
      this.dbo[stack.cmd].apply(this.dbo, stack.args);
    }));
    await Promise.all(stacks);
    this.rollback();
  }

  rollback(e) {
    if (!this.transacted) return;
    this.transacted = false;
    this.stacks = [];
    if (e) return Promise.reject(e);
  }
}

module.exports = async $plugin => {
  if (!$plugin.$config) throw new Error('redis需要配置参数');
  const app = $plugin.$app;
  const configs = Array.isArray($plugin.$config) ? $plugin.$config : [$plugin.$config];
  const dbo = configs.length > 1 ? new REDIS.Cluster(configs) : new REDIS(configs[0]);

  app.on('start', async ctx => {
    const redis = ObjectProxy(new Redis(dbo));
    ctx.on('resolve', () => redis.commit());
    ctx.on('reject', e => redis.rollback(e));
    Object.defineProperty(ctx, 'redis', { value: redis });
  });

  app.on('destroyed', async () => {
    if (configs.length > 1) {
      await dbo.quit();
    } else {
      await dbo.disconnect();
    }
  });
}

function ObjectProxy(object) {
  return new Proxy(object, {
    get(obj, key) {
      if (key in obj) {
        return Reflect.get(obj, key);
      } else {
        return obj._exec(key);
      }
    }
  })
}