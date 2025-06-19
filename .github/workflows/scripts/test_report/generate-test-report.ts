import * as core from '@actions/core';
import * as github from '@actions/github';


const acceptedWorkflows = [
    'QA - RPC Integration Tests',
    'QA - RPC Integration Tests (Polygon)',
    'QA - RPC Performance Tests',
    'QA - Snapshot Download',
    'QA - Sync from scratch',
    'QA - Sync from scratch (minimal node)',
    'QA - Sync with external CL',
    'QA - Tip tracking',
    'QA - Tip tracking (Gnosis)',
    'QA - Tip tracking (Polygon)',
    'QA - Constrained Tip tracking',
    'QA - TxPool performance test',
    'QA - Clean exit (block downloading)',
];

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
        case 'cancelled': return '⏭️';  // The run was cancelled before it completed.
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
    if (chainLowerCaseString.includes('holesky')) return '🔸 holesky';
    if (chainLowerCaseString.includes('amoy')) return '🟣 amoy';
    if (chainLowerCaseString.includes('chiado')) return '🟢 chiado';
    if (chainLowerCaseString.includes('lighthouse, gnosis')) return '🟢 gnosis / lighthouse';
    if (chainLowerCaseString.includes('gnosis')) return '🟢 gnosis';
    return chain;
}

// Removes QA and chain information from a job name
function removeQAAndChainInfo(jobName: string): string {
    return jobName
        .replace(/^QA - /, '')
        .replace(/\s*\(Polygon\)/i, '')
        .replace(/\s*\(Gnosis\)/i, '')
        .replace(/\s*\(Mainnet\)/i, '')
        .replace(/\s*\(Ethereum\)/i, '')
        .replace(/\s*\(Holesky\)/i, '')
        .replace(/\s*\(Sepolia\)/i, '')
        .replace(/\s*\(Amoy\)/i, '')
        .replace(/\s*\(Chiado\)/i, '')
        .trim();
}

// This script generates a summary of GitHub Actions workflow runs and jobs
async function run() {
    try {
        // Input
        const token = process.env.GITHUB_TOKEN as string;  // The GitHub token for authentication
        const startDate = new Date(process.env.START_DATE as string);  // The start date for filtering workflow runs
        const endDate = new Date(process.env.END_DATE as string);   // The end date for filtering workflow runs
        const branch= process.env.BRANCH_NAME ?? github.context.ref.replace(/^refs\/\w+\//, '');   // The branch name, defaults to the current branch
        //const { owner, repo } = github.context.repo;
        const {owner, repo} = {owner: 'erigontech', repo: 'erigon'};  // For testing purposes, you can hardcode the owner and repo

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

                // Skip runs that are not in the accepted workflows
                if (!acceptedWorkflows.includes(run.name ?? '')) {
                    core.info(`Skipping workflow run: ${run.name} (${run.id})`);
                    continue;
                }

                core.info(`Processing workflow run: ${run.name} (${run.id})`);

                const {data: jobsData} = await octokit.rest.actions.listJobsForWorkflowRun({
                    owner,
                    repo,
                    run_id: run.id,
                });

                // Iterate through the jobs in the workflow run
                for (const job of jobsData.jobs) {

                    const workflowName = run.name ?? run.id.toString();
                    const jobName = job.name;
                    const conclusion = mapConclusionToIcon(job.conclusion, job.status);

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

                let jobName = mapChain(workflowSummary.name)

                if (jobName == workflowSummary.name) {
                    jobName = mapChain(jobSummary.name);
                }

                let testName = removeQAAndChainInfo(workflowSummary.name)
                let row = [testName, jobName]
                let jobConclusions = [];

                for (const day of days) {
                    let dayConclusions = '';

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
        table.sort((a, b) => {
            if (typeof a[0] === 'object' && 'header' in a[0] && a[0].header === true) return 0; // Keep the header row at the top
            if (a[0] < b[0]) return -1;
            if (a[0] > b[0]) return 1;
            return 0;
        });

        // Write the summary table to the GitHub Actions summary
        await core.summary
            .addHeading('Test Report - Branch ' + branch)
            .addTable(table)
            .write();

    }
    catch (err: any) {
        core.setFailed(err.message);
    }
}

run();

// see https://github.blog/news-insights/product-news/supercharging-github-actions-with-job-summaries/