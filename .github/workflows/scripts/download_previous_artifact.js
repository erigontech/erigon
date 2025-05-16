module.exports = async ({
	github,
	context,
	core
}) => {
	const owner = context.repo.owner;
	const repo = context.repo.repo;

	const workflows = await github.rest.actions.listRepoWorkflows({
		owner,
		repo
	})

	// Extract just the filename from WORKFLOW_FILENAME and from each workflow path
	const targetFilename = process.env.WORKFLOW_FILENAME;
	const workflow = workflows.data.workflows.find(w => {
		// Extract the filename from the full path
		const pathParts = w.path.split('/');
		const workflowFilename = pathParts[pathParts.length - 1];
		return workflowFilename === targetFilename;
	});

	if (!workflow) {
		core.setFailed("No workflow found");
		return;
	}

	const runs = await github.rest.actions.listWorkflowRuns({
		owner,
		repo,
		workflow_id: workflow.id,
		status: "success",
		per_page: 1
	})

	if (runs.data.total_count === 0) {
		core.setFailed("No runs found");
		return;
	}

	const artifacts = await github.rest.actions.listWorkflowRunArtifacts({
		owner,
		repo,
		run_id: runs.data.workflow_runs[0].id
	});

	const artifact = artifacts.data.artifacts.find(artifact => artifact.name === process.env.ARTIFACT_NAME);
	if (artifact) {
		const response = await github.rest.actions.downloadArtifact({
			owner,
			repo,
			artifact_id: artifact.id,
			archive_format: 'zip'
		});
		require('fs').writeFileSync(process.env.ARTIFACT_FILENAME, Buffer.from(response.data));
		require('child_process').execSync(`unzip -o ${process.env.ARTIFACT_FILENAME} -d ${process.env.UNZIP_DIR}`);

		console.log("Artifact downloaded successfully");
	} else {
		core.setFailed("No artifact found");
	}
}