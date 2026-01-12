import * as core from '@actions/core';
import * as github from '@actions/github';

const acceptedWorkflows = [
    '.github/workflows/ci.yml',
    //'.github/workflows/lint.yml',
    //'.github/workflows/manifest.yml',
    '.github/workflows/qa-clean-exit-block-downloading.yml',
    '.github/workflows/qa-clean-exit-snapshot-downloading.yml',
    '.github/workflows/qa-constrained-tip-tracking.yml',
    '.github/workflows/qa-rpc-integration-tests-gnosis.yml',
    '.github/workflows/qa-rpc-integration-tests-latest.yml',
    '.github/workflows/qa-rpc-integration-tests-polygon.yml',
    '.github/workflows/qa-rpc-integration-tests.yml',
    '.github/workflows/qa-rpc-performance-tests.yml',
    '.github/workflows/qa-snap-download.yml',
    '.github/workflows/qa-sync-from-scratch-minimal-node.yml',
    '.github/workflows/qa-sync-from-scratch.yml',
    '.github/workflows/qa-sync-with-externalcl.yml',
    '.github/workflows/qa-tip-tracking-gnosis.yml',
    '.github/workflows/qa-tip-tracking.yml',
    '.github/workflows/qa-tip-tracking-with-load.yml',
    '.github/workflows/qa-txpool-performance-test.yml',
    '.github/workflows/test-all-erigon-race.yml',
    //'.github/workflows/test-all-erigon.yml',
    '.github/workflows/test-hive-eest.yml',
    '.github/workflows/test-hive.yml',
    '.github/workflows/test-integration-caplin.yml',
    '.github/workflows/test-kurtosis-assertoor.yml'
];

// Represents a row in the summary table, which can contain strings or header objects
type SummaryRow = (string | { data: string; header?: true })[];

// Represents a result of a job in a workflow run, containing its date, SHA, conclusion, run ID, and job ID
class JobResult {
    date!: string;
    sha!: string;
    conclusion!: string;
    runId!: number;
    jobId!: number;
}

// Represents a summary of a job within a workflow run, containing its name and results
class JobSummary {
    name!: string;
    results!: JobResult[];
}

// Represents a summary of a workflow run, containing its name and the jobs it includes
class WorkflowRunSummary {
    name!: string;
    jobs!: JobSummary[];
}

// Generates an array of date strings between two dates (inclusive)
function getDateStringsBetween(startDate: Date, endDate: Date): string[] {
    const dates: string[] = [];
    const current = new Date(startDate);

    while (current <= endDate) {
        dates.push(current.toISOString().split('T')[0]);
        current.setDate(current.getDate() + 1);
    }

    return dates;
}

// Maps GitHub Actions job conclusion to an emoji
// see https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/collaborating-on-repositories-with-code-quality-features/about-status-checks#check-statuses-and-conclusions
function mapConclusionToIcon(conclusion: string | null, status: string | null): string {
    switch (conclusion) {
        case 'success': return '✅';
        case 'failure': return '❌';
        case 'cancelled': return '🗑️️';  // The run was cancelled
        case 'cancelled_after_start': return '✖️'; // The run was cancelled before it completed.
        case 'skipped': return '⏩';  // The run was skipped.
        case 'timed_out': return '⏰️';
        case 'neutral': return '⚪️';
        case 'stale': return '🕸️';  // The run was marked stale by GitHub because it took too long.
        case 'action_required': return '⚠️';
        default: {
            switch (status) {
                case 'requested': return '📥'; // Requested
                case 'queued': return '⏳'; // Queued
                case 'waiting': return '⏸️'; // Waiting (for a deployment protection rule to be satisfied)
                case 'pending': return '🕓'; // Pending (the run is at the front of the queue but the concurrency limit has been reached)
                case 'in_progress': return '🔄'; // In progress (the run is currently running)
                case 'expected': return '🔍'; // Expected (the run is waiting for a status to be reported)
                case 'startup_failure': return '💥'; // Startup_Failure (the run failed during startup, not applicable here)
                case 'completed': return '✅'; // Completed
                case 'failed': return '❌'; // Failed
                default: return status ?? '❓'; // Unknown status
            }
        }
    }
}

function legend() {
    return `
    <ul>
        <li>${mapConclusionToIcon('success', null)} success</li>
        <li>${mapConclusionToIcon('failure', null)} failure</li>
        <li>${mapConclusionToIcon('cancelled', null)} cancelled due to a subsequent commit</li>
        <li>${mapConclusionToIcon('cancelled_after_start', null)} cancelled (manually or automatically) before completion</li>
        <li>${mapConclusionToIcon('skipped', null)} skipped</li>
        <li>${mapConclusionToIcon('timed_out', null)} timed out</li>
        <li>${mapConclusionToIcon('neutral', null)} ended with a neutral result</li>
        <li>${mapConclusionToIcon('stale', null)} it took too long</li>
        <li>${mapConclusionToIcon('action_required', null)} action required</li>
        <li>${mapConclusionToIcon(null, 'requested')} requested</li>
        <li>${mapConclusionToIcon(null, 'in_progress')} in progress</li>
        <li>${mapConclusionToIcon(null, 'queued')} waiting for a runner</li>
        <li>${mapConclusionToIcon(null, 'waiting')} waiting for a deployment protection rule to be satisfied</li>
        <li>${mapConclusionToIcon(null, 'pending')} pending (the run is at the front of the queue but the concurrency limit has been reached)</li>
        <li>${mapConclusionToIcon(null, 'expected')} expected (the run is waiting for a status to be reported)</li>
        <li>${mapConclusionToIcon(null, 'startup_failure')} startup failure (the run failed during startup, not applicable here)</li>
        <li>${mapConclusionToIcon(null, null)} unknown status or conclusion</li>
    </ul>`;
}

// To build a legend of applied conclusions and statuses
const applied_conclusions_and_statuses: { conclusion: string | null; status: string | null }[] = [];

// Modified mapConclusionToIcon to track applied conclusions and statuses
function mapConclusionToIconWithTracking(conclusion: string | null, status: string | null): string {
    // Check if this conclusion/status pair is already tracked
    const alreadyTracked = applied_conclusions_and_statuses.some(
        (item) => item.conclusion === conclusion && item.status === status
    );

    // If not tracked, add it to the list
    if (!alreadyTracked) {
        applied_conclusions_and_statuses.push({ conclusion, status });
    }

    // Return the icon using the original mapping function
    return mapConclusionToIcon(conclusion, status);}
// Maps a job name to a more readable format, including chain information
function mapChain(chain: string | null): string {
    if (!chain) return '';
    let chainLowerCaseString = chain.toLowerCase();
    if (chainLowerCaseString.includes('bor-mainnet')) return '🟣 polygon';
    if (chainLowerCaseString.includes('polygon')) return '🟣 polygon';
    if (chainLowerCaseString.includes('lighthouse, mainnet')) return '⬢ ethereum / lighthouse';
    if (chainLowerCaseString.includes('prysm, mainnet')) return '⬢ ethereum / prysm';
    if (chainLowerCaseString.includes('mainnet')) return '⬢ ethereum';
    if (chainLowerCaseString.includes('ethereum')) return '⬢ ethereum';
    if (chainLowerCaseString.includes('sepolia')) return '🔹 sepolia';
    if (chainLowerCaseString.includes('hoodi')) return '🔸 hoodi';
    if (chainLowerCaseString.includes('amoy')) return '🟣 amoy';
    if (chainLowerCaseString.includes('chiado')) return '🟢 chiado';
    if (chainLowerCaseString.includes('lighthouse, gnosis')) return '🟢 gnosis / lighthouse';
    if (chainLowerCaseString.includes('gnosis')) return '🟢 gnosis';
    return chain;
}

// Removes 'QA' and chain information from a job name
function cleanJobName(jobName: string): string {
    return jobName
        .replace(/^QA - /, '')
        .replace(/\s*\(Polygon\)/i, '')
        .replace(/\s*\(Gnosis\)/i, '')
        .replace(/\s*\(Mainnet\)/i, '')
        .replace(/\s*\(Ethereum\)/i, '')
        .replace(/\s*\(Hoodi\)/i, '')
        .replace(/\s*\(Sepolia\)/i, '')
        .replace(/\s*\(Amoy\)/i, '')
        .replace(/\s*\(Chiado\)/i, '')
        .trim();
}

// This script generates a summary of GitHub Actions workflow runs and jobs
export async function run() {
    try {
        // Input
        const token = process.env.GITHUB_TOKEN as string;  // The GitHub token for authentication
        const startDate = new Date(process.env.START_DATE as string);  // The start date for filtering workflow runs
        const endDate = new Date(process.env.END_DATE as string);   // The end date for filtering workflow runs
        // The branch name, defaults to the current branch or 'main' if not in GitHub Actions
        const branch= process.env.BRANCH_NAME ?? (github.context.ref ? github.context.ref.replace(/^refs\/\w+\//, '') : 'main');
        // Use github.context.repo if available, otherwise use default values
        const repoArray = process.env.GITHUB_REPOSITORY ? process.env.GITHUB_REPOSITORY.split('/') : ['erigontech', 'erigon'];
        const { owner, repo } = github.context.action ? github.context.repo : { owner: repoArray[0], repo: repoArray[1] };

        endDate.setUTCHours(23, 59, 59, 999);

        const created = `${startDate.toISOString().split('T')[0]}..${endDate.toISOString().split('T')[0]}`;

        // Log the inputs
        core.info(`Generating test report for branch: ${branch}`);
        core.info(`Date range: ${startDate.toISOString()} to ${endDate.toISOString()}`);
        core.info(`Using repository: ${owner}/${repo}`);

        // Initialize Octokit with the provided token
        const octokit = github.getOctokit(token);

        const summaries: WorkflowRunSummary[] = [];

        // Fetch workflow runs for the specified repository, branch, and date range and process them
        let page = 1;
        const per_page = 100;
        while (true) {
            const {data: {workflow_runs}} = await octokit.rest.actions.listWorkflowRunsForRepo({
                owner,
                repo,
                branch,
                per_page,
                page,
                created,
            });

            if (!workflow_runs.length) break;

            // Iterate through the current page of workflow runs
            for (const run of workflow_runs) {
                const runDate = new Date(run.created_at);
                if (runDate < startDate || runDate > endDate) continue;

                // Include only tests
                if (!acceptedWorkflows.includes(run.path ?? '')) {
                    core.info(`Skipping workflow run: ${run.name} (${run.id})`);
                    continue;
                }

                core.info(`Processing workflow run: ${run.name} (${run.id}) - status=${run.status}, conclusion=${run.conclusion}`);

                const {data: jobsData} = await octokit.rest.actions.listJobsForWorkflowRun({
                    owner,
                    repo,
                    run_id: run.id,
                });

                // Iterate through the jobs in the workflow run
                if (!jobsData.jobs || !jobsData.jobs.length) {
                    core.info(`No jobs found for workflow run: ${run.name} (${run.id})`);
                    continue;
                }
                for (const job of jobsData.jobs) {

                    const workflowName = run.name ?? run.id.toString();
                    const jobName = job.name;

                    // Map the job conclusion to an icon
                    let conclusion = mapConclusionToIcon(job.conclusion, job.status);

                    // Correction to treat 'cancelled' with steps differently than 'cancelled' without steps
                    if (job.conclusion === 'cancelled' && job.steps && job.steps.length > 0)
                        conclusion = mapConclusionToIcon('cancelled_after_start', job.status);

                    // Find or create the workflow summary
                    let workflowSummary = summaries.find(w => w.name === workflowName);
                    if (!workflowSummary) {
                        workflowSummary = {name: workflowName, jobs: []};
                        summaries.push(workflowSummary);
                    }

                    // Find or create the job summary
                    let jobSummary = workflowSummary.jobs.find(j => j.name === jobName);
                    if (!jobSummary) {
                        jobSummary = {name: jobName, results: []};
                        workflowSummary.jobs.push(jobSummary);
                    }

                    // Add the job result to the job summary
                    jobSummary.results.push({
                        date: runDate.toISOString().split('T')[0],
                        sha: run.head_sha,
                        conclusion,
                        runId: run.id,
                        jobId: job.id,
                    });
                }
            }

            if (workflow_runs.length < per_page) break;
            page++;
        }

        const start = new Date(startDate);
        const end = new Date(endDate);
        const days = getDateStringsBetween(start, end);

        // Prepare the summary table header
        const table: SummaryRow[] = [  // format: [Test, Job, Date1, Date2, ...]
            [ // header row
                {data: 'Test', header: true},
                {data: 'Job', header: true},
            ]
        ];

        // Add date headers to the table
        for (const day of days) {
            // extract DD-MM from day variable
            const dateParts = day.split('-');
            const formattedDate = `${dateParts[2]}-${dateParts[1]}`;
            table[0].push({data: formattedDate, header: true});
        }

        // Populate the table rows
        for (const workflowSummary of summaries) {
            for (const jobSummary of workflowSummary.jobs) {

                // Map the job name to a more readable format
                let jobName = mapChain(workflowSummary.name)
                if (jobName == workflowSummary.name) {
                    jobName = mapChain(jobSummary.name);
                }

                // order the results by date
                if (jobSummary.results.length > 0) {
                    jobSummary.results.sort(
                        (a, b) => {
                            const dateA = Date.parse(a.date);
                            const dateB = Date.parse(b.date);
                            if (isNaN(dateA) && isNaN(dateB)) return 0;
                            if (isNaN(dateA)) return 1;
                            if (isNaN(dateB)) return -1;
                            if (dateA === dateB) return a.runId - b.runId;  // If dates are equal, sort by runId
                            return dateA - dateB;
                        })
                }

                // Create a row for the job
                let testName = cleanJobName(workflowSummary.name)
                let row = [testName, jobName]
                let jobConclusions = [];

                // Fill the row with conclusions for each day
                for (const day of days) {
                    let dayConclusions = '';

                    // find results for the current day
                    for (const result of jobSummary.results) {
                        if (result.date === day) {
                            if (dayConclusions !== '') {
                                dayConclusions += ' ';
                            }
                            const jobUrl = `https://github.com/${owner}/${repo}/actions/runs/${result.runId}/job/${result.jobId}`;
                            dayConclusions += `<a href="${jobUrl}">${result.conclusion}</a>`;
                        }
                    }

                    jobConclusions.push(dayConclusions || '');
                }

                row = row.concat(jobConclusions);
                table.push(row);
            }
        }

        // Order the table by the first column (Test name) except for the header row
        const isHeaderRow = (row: SummaryRow): boolean => {
            return typeof row[0] === 'object' && 'header' in row[0] && row[0].header === true;
        };

        table.sort((a, b) => {
            // If row 'a' is the header row, it should always come before any other row
            if (isHeaderRow(a)) return -1;
            // If row 'b' is the header row, it should always come after any other row
            if (isHeaderRow(b)) return 1;
            // Otherwise, sort normally
            if (a[0] < b[0]) return -1;
            if (a[0] > b[0]) return 1;
            // If the first columns are equal (Workflow name), sort by the second column (Job name)
            if (a[1] < b[1]) return -1;
            if (a[1] > b[1]) return 1;
            return 0;
        });

        core.info(`Legend: ${legend()}`);

        // Write the summary table to the GitHub Actions summary
        await core.summary
            .addHeading('Test Report - Branch ' + branch)
            .addTable(table)
            .addDetails('Status Icon Legend', legend())
            .write();

    }
    catch (err: any) {
        core.setFailed(err.message);
    }
}
 
// If this script is run directly, execute the run function
if (import.meta.url === `file://${process.argv[1]}`) {
    run();
}

// see https://github.blog/news-insights/product-news/supercharging-github-actions-with-job-summaries/
