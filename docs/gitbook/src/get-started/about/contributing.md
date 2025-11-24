---
description: Contributing to the Erigon 3 Documentation
---

# Contributing

This documentation is powered by Gitbook but can be edited locally by using [MdBook](https://rust-lang.github.io/mdBook).

To contribute to the Erigon Docs, you can either:

1. Create an Issue: Open a new issue in the main branch to suggest changes or report problems.
2. Open a Branch and Submit a PR:
   1. Install MdBook;
   2. Create a new branch in GitHub;
   3. Before proceeding, make a backup of the `docs/gitbook/src/SUMMARY.md` file. Remove all empty lines and any lines starting with `##` to ensure compatibility with MdBook.
   4. To render the documentation locally, navigate in your terminal to the `/docs/gitbook` folder and run `mdbook serve`.
   5. Edit and preview your changes locally. Only modify content in Markdown format. Ensure all updates are verified in the local render.
   6. If all the changes are satisfactory, revert `SUMMARY.md` back to its original version.
   7. Submit a pull request (PR) on GitHub.

{% embed url="https://github.com/erigontech/erigon" %}
