// Debug script for running generate-test-report.ts
// This script sets up the necessary environment variables for TypeScript debugging

// Set up environment variables required by the script
process.env.GITHUB_TOKEN = 'your-github-token'; // Replace with your GitHub token
process.env.START_DATE = '2025-01-01'; // Replace with your desired start date
process.env.END_DATE = '2025-01-07'; // Replace with your desired end date
process.env.BRANCH_NAME = 'main'; // Replace with your desired branch name
process.env.GITHUB_REPOSITORY = 'erigontech/erigon'; // Required for github.context.repo to work

console.log("Debugging generate-test-report.ts with environment variables set.");

import { run } from './generate-test-report';

run();

