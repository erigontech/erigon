#!/bin/sh -e


echo "Please commit local changes before checking out..."
printf 'Enter commit name: ' 

read -r COMMIT

echo ""
git status 


echo ""
git add .

echo ""
git commit -m "$COMMIT"

echo ""
git checkout stable

echo ""
git pull upstream stable 

echo ""
git checkout tx_analysis

echo ""
git merge stable 

echo ""
git checkout --theirs .

echo ""
echo "--- Done..."