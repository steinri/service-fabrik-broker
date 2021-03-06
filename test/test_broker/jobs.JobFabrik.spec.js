'use strict';

const JobFabrik = require('../../broker/lib/jobs/JobFabrik');
const ScheduledBackup = require('../../broker/lib/jobs/ScheduleBackupJob');
const ScheduledOobDeploymentBackupJob = require('../../broker/lib/jobs/ScheduledOobDeploymentBackupJob');
const OperationStatusPollerJob = require('../../broker/lib/jobs/OperationStatusPollerJob');
const BnRStatusPollerJob = require('../../broker/lib/jobs/BnRStatusPollerJob');
const BluePrintJob = require('../../broker/lib/jobs/BluePrintJob');
const BackupReaperJob = require('../../broker/lib/jobs/BackupReaperJob');
const ServiceInstanceUpdateJob = require('../../broker/lib/jobs/ServiceInstanceUpdateJob');
const DbCollectionReaperJob = require('../../broker/lib/jobs/DbCollectionReaperJob');
const CONST = require('../../broker/lib/constants');
const AssertionError = require('assert').AssertionError;

describe('Jobs', function () {
  describe('JobFabrik', function () {
    describe('#getJob', function () {
      it('should return the requested Job Definition', function () {
        const backupJob = JobFabrik.getJob(CONST.JOB.SCHEDULED_BACKUP);
        expect(backupJob).to.eql(ScheduledBackup);
        const reaperJob = JobFabrik.getJob(CONST.JOB.BACKUP_REAPER);
        expect(reaperJob).to.eql(BackupReaperJob);
        const statusPollerJob = JobFabrik.getJob(CONST.JOB.OPERATION_STATUS_POLLER);
        expect(statusPollerJob).to.eql(OperationStatusPollerJob);
        const bnrStatusPollerJob = JobFabrik.getJob(CONST.JOB.BNR_STATUS_POLLER);
        expect(bnrStatusPollerJob).to.eql(BnRStatusPollerJob);
        const oobJob = JobFabrik.getJob(CONST.JOB.SCHEDULED_OOB_DEPLOYMENT_BACKUP);
        expect(oobJob).to.eql(ScheduledOobDeploymentBackupJob);
        const serviceInstanceUpdateJob = JobFabrik.getJob(CONST.JOB.SERVICE_INSTANCE_UPDATE);
        expect(serviceInstanceUpdateJob).to.eql(ServiceInstanceUpdateJob);
        const dbCollectionReaperJob = JobFabrik.getJob(CONST.JOB.DB_COLLECTION_REAPER);
        expect(dbCollectionReaperJob).to.eql(DbCollectionReaperJob);
        const blueprintJob = JobFabrik.getJob(CONST.JOB.BLUEPRINT_JOB);
        expect(blueprintJob).to.eql(BluePrintJob);
        blueprintJob.run({
          attrs: {
            data: {}
          }
        }, () => {});
      });
      it('should throw Assertion error when requested non-existing job definition', function () {
        expect(JobFabrik.getJob.bind(JobFabrik, 'NON_EXISTING_JOB')).to.throw(AssertionError);
      });
    });
  });
});