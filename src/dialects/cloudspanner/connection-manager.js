'use strict';

const AbstractConnectionManager = require('../abstract/connection-manager');
const SequelizeErrors = require('../../errors');
const { logger } = require('../../utils/logger');
const DataTypes = require('../../data-types').cloudspanner;
const debug = logger.debugContext('connection:cloudspanner');
const parserStore = require('../parserStore')('cloudspanner');

/**
 * CloudSpanner Connection Manager
 *
 * Get connections, validate and disconnect them.
 *
 * @private
 */
class ConnectionManager extends AbstractConnectionManager {
  constructor(dialect, sequelize) {
    sequelize.config.port = sequelize.config.port || 3306;
    super(dialect, sequelize);
    this.lib = this._loadDialectModule('@google-cloud/spanner');
    this.builtInPoolMgr = null;
    this.spanner = null;
    this.refreshTypeParser(DataTypes);
  }

  _refreshTypeParser(dataType) {
    parserStore.refresh(dataType);
  }

  _clearTypeParser() {
    parserStore.clear();
  }

  static _typecast(field, next) {
    if (parserStore.get(field.type)) {
      return parserStore.get(field.type)(field, this.sequelize.options, next);
    }
    return next();
  }

  /**
   * Connect with a snowflake database based on config, Handle any errors in connection
   * Set the pool handlers on connection.error
   * Also set proper timezone once connection is connected.
   *
   * @param {object} config
   * @returns {Promise<Connection>}
   * @private
   */
  async connect(config) {
    
    const connectionConfig = {
      // projectId: config.host,
      // username: config.username,
      // password: config.password,
      database: config.database,
      // warehouse: config.warehouse,
      // role: config.role,
      /*
      flags: '-FOUND_ROWS',
      timezone: this.sequelize.options.timezone,
      typeCast: ConnectionManager._typecast.bind(this),
      bigNumberStrings: false,
      supportBigNumbers: true,
      */
      ...config.dialectOptions
    };

    try {

      if (!this.database) {
        this.spanner = new this.lib.Spanner({
          projectId: connectionConfig.projectId,
          keyFilename: connectionConfig.keyFilename
        });
    
        this.instance = this.spanner.instance(connectionConfig.instanceId);
        this.database = this.instance.database(connectionConfig.database);            
      }

      debug('connection acquired');

      return this.database;
    } catch (err) {
      switch (err.code) {
        case 'ECONNREFUSED':
          throw new SequelizeErrors.ConnectionRefusedError(err);
        case 'ER_ACCESS_DENIED_ERROR':
          throw new SequelizeErrors.AccessDeniedError(err);
        case 'ENOTFOUND':
          throw new SequelizeErrors.HostNotFoundError(err);
        case 'EHOSTUNREACH':
          throw new SequelizeErrors.HostNotReachableError(err);
        case 'EINVAL':
          throw new SequelizeErrors.InvalidConnectionError(err);
        default:
          throw new SequelizeErrors.ConnectionError(err);
      }
    }
  }

  async disconnect(connection) {
    // // Don't disconnect connections with CLOSED state
    // if (!connection.isUp()) {
    //   debug('connection tried to disconnect but was already at CLOSED state');
    //   return;
    // }

    // return new Promise((resolve, reject) => {
    //   connection.destroy(err => {
    //     if (err) {
    //       console.error(`Unable to disconnect: ${err.message}`);
    //       reject(err);
    //     } else {
    //       console.log(`Disconnected connection with id: ${connection.getId()}`);
    //       resolve(connection.getId());
    //     }
    //   });
    // });
  }

  validate(connection) {
    return true;
  }
}

module.exports = ConnectionManager;
module.exports.ConnectionManager = ConnectionManager;
module.exports.default = ConnectionManager;
