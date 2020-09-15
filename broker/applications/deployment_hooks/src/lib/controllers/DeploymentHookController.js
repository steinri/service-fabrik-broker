'use strict';

const {
  errors: {
    BadRequest
  }
} = require('@sf/common-utils');
const logger = require('@sf/logger');
const HookBaseController = require('./HookBaseController');
const ActionManager = require('../actions/ActionManager');

class DeploymentHookController extends HookBaseController {
  constructor() {
    super();
  }
  // Method for getting action response
  executeActions(req, res) {
    if (!req.body.phase || !req.body.actions) {
      throw new BadRequest('Deployment phase and actions are required');
    }
    return ActionManager
      .executeActions(req.body.phase, req.body.actions, req.body.context)
      .tap(body => logger.debug('Sending response body: ', body))
      .then(body => res
        .status(200)
        .send(body));
  }
}

module.exports = DeploymentHookController;
