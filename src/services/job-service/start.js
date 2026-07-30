const JobServiceImpl = require('./impl/JobServiceImpl.js').default;

const jobService = JobServiceImpl.getInstance();
jobService.start();
