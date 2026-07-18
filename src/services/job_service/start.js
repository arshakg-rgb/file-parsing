import JobServiceImpl from './impl/JobServiceImpl.js';

const jobService = JobServiceImpl.getInstance();
jobService.start();
