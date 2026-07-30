const JobServiceManager = require('./JobServiceManager.js').default;

const jobService = JobServiceManager.getInstance();
jobService.start();
