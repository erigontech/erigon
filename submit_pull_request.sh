#!/bin/bash

echo "🚀 Submitting Pull Request for Olym3 Testnet Season 3"
echo "====================================================="

# Check if we're in the right branch
CURRENT_BRANCH=$(git branch --show-current)
echo "📋 Current branch: $CURRENT_BRANCH"

if [[ "$CURRENT_BRANCH" != "add-olym3-testnet-s3" ]]; then
    echo "❌ Not in the correct branch. Please run create_pull_request.sh first"
    exit 1
fi

# Check if there are uncommitted changes
if ! git diff --quiet; then
    echo "📝 Committing changes..."
    git add .
    git commit -m "Add Olym3 Testnet Season 3

- Add chain specification for olym3-testnet-s3
- Chain ID: 256003 (0x3e803)
- Ethash consensus with PoS transition
- All Ethereum forks activated from block 0
- Pre-funded test accounts for development
- Gas limit: 60,000,000
- Ready for smart contract development and testing"
else
    echo "✅ No uncommitted changes"
fi

# Push the branch
echo "📤 Pushing branch to origin..."
git push origin add-olym3-testnet-s3

if [ $? -eq 0 ]; then
    echo "✅ Branch pushed successfully"
    echo ""
    echo "🎯 Next steps:"
    echo "=============="
    echo "1. Go to: https://github.com/erigontech/erigon/compare"
    echo "2. Select 'compare across forks'"
    echo "3. Base repository: erigontech/erigon (main branch)"
    echo "4. Head repository: thanhnhaweb3/erigon (add-olym3-testnet-s3 branch)"
    echo "5. Click 'Create pull request'"
    echo ""
    echo "📝 PR Title: Add Olym3 Testnet Season 3 (Chain ID: 256003)"
    echo ""
    echo "📋 PR Description (copy and paste):"
    echo "===================================="
    cat pr_description.md
    echo ""
    echo "🔗 Direct link to create PR:"
    echo "https://github.com/erigontech/erigon/compare/main...thanhnhaweb3:erigon:add-olym3-testnet-s3"
    echo ""
    echo "✅ Ready to submit Pull Request!"
else
    echo "❌ Failed to push branch"
    echo "Please check your git configuration and try again"
fi
