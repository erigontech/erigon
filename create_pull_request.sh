#!/bin/bash

echo "🚀 Creating Pull Request for Olym3 Testnet Season 3"
echo "=================================================="

# Check if we're in the right repository
CURRENT_REPO=$(git remote get-url origin)
echo "📋 Current repository: $CURRENT_REPO"

if [[ "$CURRENT_REPO" == *"thanhnhaweb3/erigon"* ]]; then
    echo "✅ Currently in your fork repository"
else
    echo "❌ Not in the correct repository"
    echo "Please make sure you're in your fork of erigon"
    exit 1
fi

# Check if we have the official erigon as upstream
echo "🔍 Checking upstream repository..."
if git remote get-url upstream >/dev/null 2>&1; then
    echo "✅ Upstream repository already configured"
else
    echo "🔧 Adding upstream repository..."
    git remote add upstream https://github.com/erigontech/erigon.git
fi

# Fetch latest changes from upstream
echo "📥 Fetching latest changes from upstream..."
git fetch upstream

# Create a new branch for the pull request
BRANCH_NAME="add-olym3-testnet-s3"
echo "🌿 Creating branch: $BRANCH_NAME"
git checkout -b $BRANCH_NAME

# Ensure we're up to date with upstream main
echo "🔄 Syncing with upstream main..."
git merge upstream/main

# Check if there are any conflicts
if [ $? -ne 0 ]; then
    echo "❌ Merge conflicts detected. Please resolve them manually."
    echo "Run: git status to see conflicted files"
    exit 1
fi

echo "✅ Branch created and synced with upstream"
echo ""
echo "📋 Files that will be included in the PR:"
echo "=========================================="
echo "1. execution/chain/spec/chainspecs/olym3-testnet-s3.json"
echo "2. execution/chain/spec/allocs/olym3-testnet-s3.json"
echo "3. execution/chain/spec/genesis.go"
echo "4. execution/chain/spec/network_id.go"
echo "5. execution/chain/networkname/network_name.go"
echo "6. execution/chain/spec/config.go"
echo "7. turbo/node/node.go"
echo ""
echo "🎯 Next steps:"
echo "1. Review the changes: git diff upstream/main"
echo "2. Commit the changes: git add . && git commit -m 'Add Olym3 Testnet Season 3'"
echo "3. Push the branch: git push origin $BRANCH_NAME"
echo "4. Create PR on GitHub: https://github.com/erigontech/erigon/compare"
echo ""
echo "📝 PR Title: Add Olym3 Testnet Season 3 (Chain ID: 256003)"
echo "📝 PR Description:"
echo "   This PR adds support for Olym3 Testnet Season 3 with Chain ID 256003."
echo "   "
echo "   Features:"
echo "   - Chain ID: 256003 (0x3e803)"
echo "   - Chain Name: olym3-testnet-s3"
echo "   - Ethash consensus with PoS transition"
echo "   - All Ethereum forks activated from block 0"
echo "   - Pre-funded test accounts for development"
echo "   - Gas limit: 60,000,000"
echo "   "
echo "   This testnet is designed for:"
echo "   - Smart contract development and testing"
echo "   - DApp development and deployment"
echo "   - Blockchain education and research"
echo "   - Community testing and validation"
