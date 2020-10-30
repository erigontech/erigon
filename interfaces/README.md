# Interfaces
Interfaces for turbo-geth components. Currently it is a collection of `.proto` files describing gRPC interfaces between components, but later documentation about each interface, its components, as well as required version of gRPC will be added

# Suggested integration into other repositories
```
git subtree add --prefix interfaces --squash https://github.com/ledgerwatch/interfaces master
```

When you need to update the subtree to a specific commit or tag, you can use this command:

```
git subtree pull --prefix interfaces --squash https://github.com/ledgerwatch/interfaces <tag_or_commit>
```
