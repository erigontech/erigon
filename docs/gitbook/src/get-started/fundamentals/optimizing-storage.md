---
description: Using fast disks and cheap disks
---

# Optimizing Storage

For optimal performance, it's recommended to store the datadir on a fast NVMe-RAID disk. However, if this is not feasible, you can store the history on a cheaper disk and still achieve good performance.

{% stepper %}
{% step %}
#### Store datadir on the slow disk

Place the `datadir` on the slower disk. Then, create symbolic links (using `ln -s`) to the **fast disk** for the following sub-folders:

* `chaindata`
* `snapshots/domain`

This will speed up the execution of E3.

On the **slow disk** place `datadir` folder with the following structure:

* `chaindata` (linked to fast disk)
* `snapshots`
  * `domain` (linked to fast disk)
  * `history`
  * `idx`
  * `accessor`
* `temp`
{% endstep %}

{% step %}
#### Speed Up History Access (Optional)

If you need to further improve performance try the following improvements step by step:

1. Store the `snapshots/accessor` folder on the fast disk. This should provide a noticeable speed boost.
2. If the speed is still not satisfactory, move the `snapshots/idx` folder to the fast disk.
3. If performance is still an issue, consider moving the entire `snapshots/history` folder to the fast disk.

By following these steps, you can optimize your Erigon 3 storage setup to achieve a good balance between performance and cost.
{% endstep %}
{% endstepper %}
