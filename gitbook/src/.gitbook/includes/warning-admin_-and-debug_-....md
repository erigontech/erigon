---
title: 'Warning: admin_ and debug_ ...'
---

{% hint style="danger" %}
**Warning**: `admin_` and `debug_` namespaces are not intended to be publicly accessible. To prevent unauthorized access, you must not expose RPC daemon port to the Internetif you are using these namespaces. For environments serving both public and private requests, we strongly recommend running a second, private RPC daemon exclusively for your internal `admin_` and `debug_` requests.
{% endhint %}
