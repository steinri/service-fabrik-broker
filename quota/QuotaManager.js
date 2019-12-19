'use strict';

const _ = require('lodash');
const Promise = require('bluebird');
const logger = require('../common/logger');
const cf = require('../data-access-layer/cf');
const eventmesh = require('../data-access-layer/eventmesh');
const catalog = require('../common/models/catalog');
const config = require('../common/config');
const CONST = require('../common/constants');

const errors = require('../common/errors');
const BadRequest = errors.BadRequest;
const Forbidden = errors.Forbidden;

const QUOTA_CR = {
  RESOURCE_GROUP: 'deployment.abap.ondemand.com',
  RESOURCE_TYPE: 'instancequotas',
  NAMESPACE: 'default',
  API_VERSION: 'v1alpha1',
};

class QuotaManager {
  constructor(quotaAPIClient) {
    this.quotaAPIClient = quotaAPIClient;
  }
  async checkQuota(req, plan, orgId) {
    const quotaType = _.get(plan, 'quota.type');
    switch (quotaType) {
      case 'abap': {
        const quotaOptions = _.get(plan, 'quota.options', {});
        return this.checkQuotaAbap(req, quotaOptions, orgId)
      }
      case 'default': default: {
        return this.checkQuotaDefault(orgId, req.body.plan_id, _.get(req, 'body.previous_values.plan_id'), req.method)
      }
    }
  }

  async checkQuotaAbap(req, quotaOptions, orgId) {
    const quotaType = 'abap';
    const quotaCodeAcu = 'abap_compute_unit';
    const quotaCodeHcu = 'hana_compute_unit';

    const isUpdate = CONST.HTTP_METHOD.PATCH === req.method;

    const planId = req.body.plan_id;
    const plan = _.find(catalog.plans, ['id', planId]);
    const managerContext = _.get(plan, 'manager.settings.context', {});
    // TODO verify that no special handling for plan changes is necessary
    // const isPlanChange = isUpdate && planId !== _.get(req, 'body.previous_values.plan_id');

    let requestedAcu = _.get(req, 'body.parameters.size_of_runtime');
    let requestedHcu = _.get(req, 'body.parameters.size_of_persistence');
    if (!isUpdate && (!requestedAcu || !requestedHcu)) {
      throw new BadRequest(`The parameters size_of_runtime and size_of_persistence must be provided, but are missing.`);
    }

    const loadingQuotaResourceOfInstance = (isUpdate) ? eventmesh.apiServerClient.getResource({
      resourceGroup: QUOTA_CR.RESOURCE_GROUP,
      resourceType: QUOTA_CR.RESOURCE_TYPE,
      resourceId: req.params.instance_id,
      namespaceId: 'default',
    }) : Promise.resolve({});

    const [allowedAcu, allowedHcu, quotaResourcesOfSubaccount, quotaResourceOfInstance] = await Promise.all([
      this.quotaAPIClient.getQuota(orgId, quotaType, quotaCodeAcu),
      this.quotaAPIClient.getQuota(orgId, quotaType, quotaCodeHcu),
      eventmesh.apiServerClient.getResources({
        resourceGroup: QUOTA_CR.RESOURCE_GROUP,
        resourceType: QUOTA_CR.RESOURCE_TYPE,
        namespaceId: 'default',
        query: {
          labelSelector: `abap.ondemand.com/organizationId=${orgId}`,
        }
      }),
      loadingQuotaResourceOfInstance,
    ]);

    const consumedAcu = quotaResourcesOfSubaccount.reduce((acc, val) => acc + _.get(val, 'spec.acu', 0), 0);
    const consumedHcu = quotaResourcesOfSubaccount.reduce((acc, val) => acc + _.get(val, 'spec.hcu', 0), 0);

    const consumedAcuOfInstance = (isUpdate) ? _.get(quotaResourceOfInstance, 'spec.acu', 0) : 0;
    const consumedHcuOfInstance = (isUpdate) ? _.get(quotaResourceOfInstance, 'spec.hcu', 0) : 0;

    // in the update case take the acu and hcu from the existing instance if they are not given
    if (isUpdate) {
      if (!requestedAcu) {
        requestedAcu = consumedAcuOfInstance;
      }
      if (!requestedHcu) {
        requestedHcu = consumedHcuOfInstance;
      }
      // hcu changes are not possible at the moment!
      if (requestedHcu !== consumedHcuOfInstance) {
        throw new BadRequest(`Not allowed to change size_of_persistence, requested: ${requestedHcu}, used: ${consumedHcuOfInstance}`);
      }
    }

    logger.debug(`ACU: requested: ${requestedAcu}, consumed: ${consumedAcu}, allowed: ${allowedAcu}`);
    logger.debug(`HCU: requested: ${requestedHcu}, consumed: ${consumedHcu}, allowed: ${allowedHcu}`);

    // verify that the request acu and hcu is valid
    const validAcus = _.get(managerContext, 'valid_acus', null);
    const validHcus = _.get(managerContext, 'valid_hcus', null);
    if (validAcus && !_.includes(validAcus, requestedAcu)) {
      throw new BadRequest(`The requested size_of_runtime is not valid for this service plan, requested: ${requestedAcu}, valid values are ${JSON.stringify(validAcus)}`);
    }
    if (validHcus && !_.includes(validHcus, requestedHcu)) {
      throw new BadRequest(`The requested size_of_persistence is not valid for this service plan, requested: ${requestedHcu}, valid values are ${JSON.stringify(validHcus)}`);
    }

    if (consumedAcu + requestedAcu - consumedAcuOfInstance > allowedAcu || consumedHcu + requestedHcu - consumedHcuOfInstance > allowedHcu) {
      const message = isUpdate
        ? `Quota is not sufficient for this request.\n\tRuntime - requested: change ${consumedAcuOfInstance} to ${requestedAcu}, currently used: ${consumedAcu}, limit: ${allowedAcu};\n\tPersistence - requested:  change ${consumedHcuOfInstance} to ${requestedHcu}, currently used: ${consumedHcu}, limit: ${allowedHcu}`
        : `Quota is not sufficient for this request.\n\tRuntime - requested: ${requestedAcu}, currently used: ${consumedAcu}, limit: ${allowedAcu};\n\tPersistence - requested: ${requestedHcu}, currently used: ${consumedHcu}, limit: ${allowedHcu}`;
      throw new Forbidden(message);
    }

    return CONST.QUOTA_API_RESPONSE_CODES.VALID_QUOTA;
  }

  checkQuotaDefault(orgId, planId, previousPlanId, reqMethod) {
    return Promise.try(() => {
      if (CONST.HTTP_METHOD.PATCH === reqMethod && this.isSamePlanOrSkuUpdate(planId, previousPlanId)) {
        logger.debug('Quota check skipped as it is a normal instance update or plan update with same sku.');
        return CONST.QUOTA_API_RESPONSE_CODES.VALID_QUOTA;
      } else {
        return this.isOrgWhitelisted(orgId)
          .then(isWhitelisted => {
            if (isWhitelisted) {
              logger.debug('Org whitelisted, Quota check skipped');
              return CONST.QUOTA_API_RESPONSE_CODES.VALID_QUOTA;
            } else {
              logger.debug(`Org id is ${orgId}`);
              logger.debug(`Plan id is ${planId}`);
              let planName = _.find(catalog.plans, ['id', planId]).name;
              let serviceName = _.find(catalog.plans, ['id', planId]).service.name;
              let skipQuotaCheck = _.find(catalog.plans, ['id', planId]).metadata ? _.find(catalog.plans, ['id', planId]).metadata.skip_quota_check : undefined;
              logger.debug(`Plan Name is ${planName}`);
              logger.debug(`Service Name is ${serviceName}`);
              logger.debug(`Skip Quota check: ${skipQuotaCheck}`);
              if (skipQuotaCheck && skipQuotaCheck === true) {
                return CONST.QUOTA_API_RESPONSE_CODES.VALID_QUOTA;
              } else {
                const planIdsWithSameSKU = this.getAllPlanIdsWithSameSKU(planName, serviceName, catalog);
                return this.quotaAPIClient.getQuota(orgId, serviceName, planName)
                  .then(quota => {
                    // Special cases:
                    // When obtained quota = 0, send message to customer – Not entitled to create service instance
                    // When obtained quota = -1, assume that the org is whitelisted and hence allow the creation
                    if (quota === 0) {
                      return CONST.QUOTA_API_RESPONSE_CODES.NOT_ENTITLED;
                    } else if (quota === -1) {
                      return CONST.QUOTA_API_RESPONSE_CODES.VALID_QUOTA;
                    } else {
                      return this.getAllPlanGuidsFromPlanIDs(planIdsWithSameSKU)
                        .tap(planGuids => logger.debug('planguids are ', planGuids))
                        .then(planGuids => cf.cloudController.getServiceInstancesInOrgWithPlansGuids(orgId, planGuids))
                        .tap(instances => logger.debug(`Number of instances are ${_.size(instances)} & Quota number for current org space and service_plan is ${quota}`))
                        .then(instances => _.size(instances) >= quota ? CONST.QUOTA_API_RESPONSE_CODES.INVALID_QUOTA : CONST.QUOTA_API_RESPONSE_CODES.VALID_QUOTA);
                    }
                  });
              }
            }
          });
      }
    });
  }

  getAllPlanIdsWithSameSKU(planName, serviceName, serviceCatalog) {
    return Promise.try(() => {
      const planManagerName = _.find(catalog.plans, ['name', planName]).manager.name;
      const skuName = this.getSkuNameForPlan(planName);

      logger.debug(`SKUName is ${skuName}`);
      const planIdsWithSameSKU = [];
      const service = _.find(serviceCatalog.services, ['name', serviceName]);
      _.each(service.plans, plan => {
        if (plan.name.endsWith(skuName) && plan.manager.name === planManagerName) {
          planIdsWithSameSKU.push(plan.id);
          logger.debug(`Found a plan with name as ${plan.name} which contains the skuName ${skuName}`);
        }
      });
      logger.debug('sameskuplanids are ', planIdsWithSameSKU);
      return planIdsWithSameSKU;
    });
  }

  isOrgWhitelisted(orgId) {
    return cf.cloudController.getOrganization(orgId)
      .tap(org => {
        logger.debug('current org details are ', org);
        logger.debug('current org name is ', org.entity.name);
        logger.debug('Whitelisted orgs are ', config.quota.whitelist);
      })
      .then(org => _.includes(config.quota.whitelist, org.entity.name));
  }

  getAllPlanGuidsFromPlanIDs(planIds) {
    return Promise.map(planIds, planId => this.getPlanGuidFromPlanID(planId));
  }

  getPlanGuidFromPlanID(planId) {
    return cf.cloudController.getServicePlans(`unique_id:${planId}`)
      .tap(plans => logger.debug(`planguid for uniqueid ${planId} is ${_.head(plans).metadata.guid}`))
      .then(plans => _.head(plans).metadata.guid);
  }

  isSamePlanOrSkuUpdate(planId, previousPlanId) {
    return previousPlanId === undefined || planId === undefined || previousPlanId === planId || this.getSkuNameForPlan(_.find(catalog.plans, ['id', previousPlanId]).name) === this.getSkuNameForPlan(_.find(catalog.plans, ['id', planId]).name);
  }

  getSkuNameForPlan(planName) {
    const firstIdx = planName.indexOf('-'); // assumption here is that service plan names are versioned, and the format is like <version>-{...}-<tshirt-size>
    return planName.substring(planName.indexOf('-', firstIdx)); // and skuName will be only -{...}-<tshirt-size>
  }
}

module.exports = QuotaManager;
