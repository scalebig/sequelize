'use strict';

const momentTz = require('moment-timezone');
const moment = require('moment');

module.exports = BaseTypes => {
  BaseTypes.ABSTRACT.prototype.dialectTypes = 'https://cloud.google.com/spanner/docs/reference/standard-sql/data-types';

  /**
   * types: [buffer_type, ...]
   *
   */

  BaseTypes.DATE.types.cloudspanner = ['TIMESTAMP'];
  BaseTypes.STRING.types.cloudspanner = ['STRING'];
  BaseTypes.CHAR.types.cloudspanner = ['STRING'];
  BaseTypes.TEXT.types.cloudspanner = ['STRING'];
  BaseTypes.TINYINT.types.cloudspanner = ['INT64'];
  BaseTypes.SMALLINT.types.cloudspanner = ['INT64'];
  BaseTypes.MEDIUMINT.types.cloudspanner = ['INT64'];
  BaseTypes.INTEGER.types.cloudspanner = ['INT64'];
  BaseTypes.BIGINT.types.cloudspanner = ['INT64'];
  BaseTypes.FLOAT.types.cloudspanner = ['FLOAT64'];
  BaseTypes.TIME.types.cloudspanner = ['TIME'];
  BaseTypes.DATEONLY.types.cloudspanner = ['DATE'];
  BaseTypes.BOOLEAN.types.cloudspanner = ['BOOL'];
  BaseTypes.BLOB.types.cloudspanner = ['BYTES'];
  BaseTypes.DECIMAL.types.cloudspanner = ['NUMERIC'];
  BaseTypes.UUID.types.cloudspanner = false;
  BaseTypes.ENUM.types.cloudspanner = false;
  BaseTypes.REAL.types.cloudspanner = ['FLOAT64'];
  BaseTypes.DOUBLE.types.cloudspanner = ['FLOAT64'];
  BaseTypes.GEOMETRY.types.cloudspanner = false;
  BaseTypes.JSON.types.cloudspanner = ['JSON'];
  BaseTypes.ARRAY.types.cloudspanner = ['ARRAY'];

  class STRING extends BaseTypes.STRING {
    toSql() {
      return `STRING(${ this._length })`;
    }
    
    _stringify(value, options) {
      return options.escape(value);
    }
  }

  STRING.prototype.escape = false;

  class INTEGER extends BaseTypes.INTEGER {
    toSql() {
      return 'INT64';
    }

    constructor(length) {
      super(length);
    }

    static parse(value) {
      return parseInt(value, 10);
    }
  }

  class DATE extends BaseTypes.DATE {
    toSql() {
      return 'TIMESTAMP';
    }
    _stringify(date, options) {
      if (!moment.isMoment(date)) {
        date = this._applyTimezone(date, options);
      }
      if (this._length) {
        return date.format('YYYY-MM-DD HH:mm:ss.SSS');
      }
      return date.format('YYYY-MM-DD HH:mm:ss');
    }
    static parse(value, options) {
      value = value.string();
      if (value === null) {
        return value;
      }
      if (momentTz.tz.zone(options.timezone)) {
        value = momentTz.tz(value, options.timezone).toDate();
      }
      else {
        value = new Date(`${value} ${options.timezone}`);
      }
      return value;
    }
  }

  class DATEONLY extends BaseTypes.DATEONLY {
    static parse(value) {
      return value.string();
    }
  }
  class UUID extends BaseTypes.UUID {
    toSql() {
      return 'STRING(36)';
    }
  }

  class TEXT extends BaseTypes.TEXT {
    toSql() {
      return 'TEXT';
    }
  }

  class BOOLEAN extends BaseTypes.BOOLEAN {
    toSql() {
      return 'BOOLEAN';
    }
  }

  class JSONTYPE extends BaseTypes.JSON {
    _stringify(value, options) {
      return options.operation === 'where' && typeof value === 'string' ? value : JSON.stringify(value);
    }
  }

  return {
    INTEGER,
    STRING,
    TEXT,
    DATE,
    BOOLEAN,
    DATEONLY,
    UUID,
    JSON: JSONTYPE
  };
};
