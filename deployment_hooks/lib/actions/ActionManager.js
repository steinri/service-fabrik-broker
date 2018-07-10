'use strict';

const fs = require('fs');
const _ = require('lodash');
const Promise = require('bluebird');
const path = require('path');
const logger = require('../../../common/logger');
const errors = require('../errors');
const CONST = require('../constants');
const ScriptExecutor = require('../utils/ScriptExecutor');

class ActionManager {
  static getAction(phase, action) {
    try {
      const actionProcessor = require(`./js/${action}`);
      let actionHandler = actionProcessor[`execute${phase}`];
      if (typeof actionHandler !== 'function') {
        return this.voidImplementation(action, phase, 'JS');
      }
      const execute = function () {
        logger.info(`Invoking handler ${action} ${phase} with args..`, arguments);
        const jsExecuterAbsPath = path.join(__dirname, 'JSExecutor.js');
        const command = `$NODE_CMD ${jsExecuterAbsPath} ${action} ${phase}`;
        const executor = new ScriptExecutor(command);
        return executor.execute.apply(executor, arguments);
      };
      return execute;
    } catch (err) {
      if (err.code !== 'MODULE_NOT_FOUND') {
        throw err;
      }
      const actionScriptAbsPath = path.join(__dirname, 'sh', `${action}_${phase}`);
      logger.info(`Script path... ${actionScriptAbsPath}`);
      if (fs.existsSync(actionScriptAbsPath)) {
        logger.info(`action ${action} for phase ${phase} is defined in the script ${action}_${phase}`);
        const execute = function () {
          const command = actionScriptAbsPath;
          const executor = new ScriptExecutor(command);
          return executor.execute.apply(executor, arguments);
        };
        return execute;
      } else {
        logger.warn(`action ${action} for phase ${phase} undefined`);
        const otherPhases = _.chain(CONST.SERVICE_LIFE_CYCLE)
          .values()
          .filter((value) => value !== phase)
          .value();
        let scriptDefinedForOtherPhases = false;
        _.each(otherPhases, (otherPhase) => {
          const actionScriptAbsPath = path.join(__dirname, 'sh', `${action}_${otherPhase}`);
          logger.info(`Script path .. ${actionScriptAbsPath}`);
          if (fs.existsSync(actionScriptAbsPath)) {
            scriptDefinedForOtherPhases = true;
            return false;
          }
        });
        if (scriptDefinedForOtherPhases) {
          //action must be defined atleast for one phase.
          return this.voidImplementation(action, phase, 'script');
        }
        throw new errors.NotImplemented(`Not implemented ${phase} for ${action}`);
      }
    }
  }

  static voidImplementation(action, phase, type) {
    logger.info(`action ${action} for phase ${phase} undefined in the ${type} : ${action}. Void implementation is being returned back`);
    const actionHandler = () => {
      logger.warn(`Not implemented ${phase} for ${action}`);
      return Promise.resolve(0);
    };
    return actionHandler;
  }

  static executeActions(phase, actions, context) {
    const actionResponse = {};
    return Promise.map(actions, (action) => {
        logger.debug(`Looking up action ${action}`);
        const actionHandler = this.getAction(phase, action);
        return actionHandler(context)
          .tap(resp => actionResponse[action] = resp);
      })
      .return(actionResponse);
  }
}

module.exports = ActionManager;