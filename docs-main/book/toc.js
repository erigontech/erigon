// Populate the sidebar
//
// This is a script, and not included directly in the page, to control the total size of the book.
// The TOC contains an entry for each page, so if each page includes a copy of the TOC,
// the total size of the page becomes O(n**2).
class MDBookSidebarScrollbox extends HTMLElement {
    constructor() {
        super();
    }
    connectedCallback() {
        this.innerHTML = '<ol class="chapter"><li class="chapter-item expanded affix "><li class="spacer"></li><li class="chapter-item expanded affix "><li class="part-title">Erigon 3 docs summary</li><li class="chapter-item expanded "><a href="introduction/introduction.html"><strong aria-hidden="true">1.</strong> Introduction</a><a class="toggle"><div>❱</div></a></li><li><ol class="section"><li class="chapter-item "><a href="introduction/why-using-erigon.html"><strong aria-hidden="true">1.1.</strong> Why using Erigon?</a></li><li class="chapter-item "><a href="introduction/contributing.html"><strong aria-hidden="true">1.2.</strong> Contributing</a></li></ol></li><li class="chapter-item expanded "><a href="getting-started/getting-started.html"><strong aria-hidden="true">2.</strong> Getting Started</a><a class="toggle"><div>❱</div></a></li><li><ol class="section"><li class="chapter-item "><a href="getting-started/hardware-requirements.html"><strong aria-hidden="true">2.1.</strong> Hardware Requirements</a></li><li class="chapter-item "><a href="getting-started/software-requirements.html"><strong aria-hidden="true">2.2.</strong> Software Requirements</a></li><li class="chapter-item "><a href="getting-started/installation.html"><strong aria-hidden="true">2.3.</strong> Installation</a><a class="toggle"><div>❱</div></a></li><li><ol class="section"><li class="chapter-item "><a href="getting-started/installation/linux-and-macos.html"><strong aria-hidden="true">2.3.1.</strong> Linux and MacOS</a><a class="toggle"><div>❱</div></a></li><li><ol class="section"><li class="chapter-item "><a href="getting-started/installation/pre-built-binaries.html"><strong aria-hidden="true">2.3.1.1.</strong> Pre-built binaries</a></li><li class="chapter-item "><a href="getting-started/installation/build-Erigon-from-source.html"><strong aria-hidden="true">2.3.1.2.</strong> Build Erigon from source</a></li></ol></li><li class="chapter-item "><a href="getting-started/installation/windows.html"><strong aria-hidden="true">2.3.2.</strong> Windows</a><a class="toggle"><div>❱</div></a></li><li><ol class="section"><li class="chapter-item "><a href="getting-started/installation/windows-build-executables.html"><strong aria-hidden="true">2.3.2.1.</strong> Build executable binaries natively for Windows</a></li><li class="chapter-item "><a href="getting-started/installation/windows-wsl.html"><strong aria-hidden="true">2.3.2.2.</strong> Windows Subsystem for Linux (WSL)</a></li></ol></li><li class="chapter-item "><a href="getting-started/installation/docker.html"><strong aria-hidden="true">2.3.3.</strong> Docker</a></li><li class="chapter-item "><a href="getting-started/installation/upgrading.html"><strong aria-hidden="true">2.3.4.</strong> Upgrading from a previous version</a></li><li class="chapter-item "><a href="getting-started/installation/migrating-from-geth.html"><strong aria-hidden="true">2.3.5.</strong> Migrating from Geth</a></li></ol></li></ol></li><li class="chapter-item expanded "><a href="easy-nodes/easy-nodes.html"><strong aria-hidden="true">3.</strong> Easy Nodes</a><a class="toggle"><div>❱</div></a></li><li><ol class="section"><li class="chapter-item "><a href="easy-nodes/how-to-run-an-ethereum-node.html"><strong aria-hidden="true">3.1.</strong> How to run a Ethereum node</a><a class="toggle"><div>❱</div></a></li><li><ol class="section"><li class="chapter-item "><a href="easy-nodes/ethereum-with-an-external-cl.html"><strong aria-hidden="true">3.1.1.</strong> Ethereum with an external CL</a></li></ol></li><li class="chapter-item "><a href="easy-nodes/how-to-run-a-gnosis-chain-node.html"><strong aria-hidden="true">3.2.</strong> How to run a Gnosis Chain node</a><a class="toggle"><div>❱</div></a></li><li><ol class="section"><li class="chapter-item "><a href="easy-nodes/gnosis-with-an-external-cl.html"><strong aria-hidden="true">3.2.1.</strong> Gnosis Chain with an external CL</a></li></ol></li><li class="chapter-item "><a href="easy-nodes/how-to-run-a-polygon-node.html"><strong aria-hidden="true">3.3.</strong> How to run a Polygon node</a></li></ol></li><li class="chapter-item expanded "><a href="fundamentals/fundamentals.html"><strong aria-hidden="true">4.</strong> Fundamentals</a><a class="toggle"><div>❱</div></a></li><li><ol class="section"><li class="chapter-item "><a href="fundamentals/basic-usage.html"><strong aria-hidden="true">4.1.</strong> Basic Usage</a></li><li class="chapter-item "><a href="fundamentals/configuring-erigon.html"><strong aria-hidden="true">4.2.</strong> Configuring Erigon</a></li><li class="chapter-item "><a href="fundamentals/supported-networks.html"><strong aria-hidden="true">4.3.</strong> Supported Networks</a></li><li class="chapter-item "><a href="fundamentals/layer-2-networks.html"><strong aria-hidden="true">4.4.</strong> Layer 2 Networks</a></li><li class="chapter-item "><a href="fundamentals/default-ports.html"><strong aria-hidden="true">4.5.</strong> Default ports</a></li><li class="chapter-item "><a href="fundamentals/sync-modes.html"><strong aria-hidden="true">4.6.</strong> Sync Modes</a></li><li class="chapter-item "><a href="fundamentals/caplin.html"><strong aria-hidden="true">4.7.</strong> Caplin</a></li><li class="chapter-item "><a href="fundamentals/optimizing-storage.html"><strong aria-hidden="true">4.8.</strong> Optimizing Storage</a></li><li class="chapter-item "><a href="fundamentals/logs.html"><strong aria-hidden="true">4.9.</strong> Logs</a></li><li class="chapter-item "><a href="fundamentals/security.html"><strong aria-hidden="true">4.10.</strong> Security</a></li><li class="chapter-item "><a href="fundamentals/jwt.html"><strong aria-hidden="true">4.11.</strong> JWT Secret</a></li><li class="chapter-item "><a href="fundamentals/tls-authentication.html"><strong aria-hidden="true">4.12.</strong> TLS Authentication</a></li><li class="chapter-item "><a href="fundamentals/performance-tricks.html"><strong aria-hidden="true">4.13.</strong> Performance Tricks</a></li><li class="chapter-item "><a href="fundamentals/modules/modules.html"><strong aria-hidden="true">4.14.</strong> Modules</a><a class="toggle"><div>❱</div></a></li><li><ol class="section"><li class="chapter-item "><a href="fundamentals/modules/rpc-daemon.html"><strong aria-hidden="true">4.14.1.</strong> RPC Daemon</a></li><li class="chapter-item "><a href="fundamentals/modules/txpool.html"><strong aria-hidden="true">4.14.2.</strong> TxPool</a></li><li class="chapter-item "><a href="fundamentals/modules/sentry.html"><strong aria-hidden="true">4.14.3.</strong> Sentry</a></li><li class="chapter-item "><a href="fundamentals/modules/downloader.html"><strong aria-hidden="true">4.14.4.</strong> Downloader</a></li></ol></li><li class="chapter-item "><a href="fundamentals/multiple-instances.html"><strong aria-hidden="true">4.15.</strong> Multiple instances / One machine</a></li><li class="chapter-item "><a href="fundamentals/web3-wallet.html"><strong aria-hidden="true">4.16.</strong> Web3 Wallet</a></li></ol></li><li class="chapter-item expanded "><a href="interacting-with-erigon/interacting-with-erigon.html"><strong aria-hidden="true">5.</strong> Interacting with Erigon</a><a class="toggle"><div>❱</div></a></li><li><ol class="section"><li class="chapter-item "><a href="interacting-with-erigon/eth.html"><strong aria-hidden="true">5.1.</strong> eth</a></li><li class="chapter-item "><a href="interacting-with-erigon/erigon.html"><strong aria-hidden="true">5.2.</strong> erigon</a></li><li class="chapter-item "><a href="interacting-with-erigon/web3.html"><strong aria-hidden="true">5.3.</strong> web3</a></li><li class="chapter-item "><a href="interacting-with-erigon/net.html"><strong aria-hidden="true">5.4.</strong> net</a></li><li class="chapter-item "><a href="interacting-with-erigon/debug.html"><strong aria-hidden="true">5.5.</strong> debug</a></li><li class="chapter-item "><a href="interacting-with-erigon/trace.html"><strong aria-hidden="true">5.6.</strong> trace</a></li><li class="chapter-item "><a href="interacting-with-erigon/txpool.html"><strong aria-hidden="true">5.7.</strong> txpool</a></li><li class="chapter-item "><a href="interacting-with-erigon/admin.html"><strong aria-hidden="true">5.8.</strong> admin</a></li><li class="chapter-item "><a href="interacting-with-erigon/bor.html"><strong aria-hidden="true">5.9.</strong> bor</a></li><li class="chapter-item "><a href="interacting-with-erigon/ots.html"><strong aria-hidden="true">5.10.</strong> ots</a></li><li class="chapter-item "><a href="interacting-with-erigon/internal.html"><strong aria-hidden="true">5.11.</strong> internal</a></li><li class="chapter-item "><a href="interacting-with-erigon/grpc.html"><strong aria-hidden="true">5.12.</strong> gRPC</a></li></ol></li><li class="chapter-item expanded "><a href="staking/staking.html"><strong aria-hidden="true">6.</strong> Staking</a><a class="toggle"><div>❱</div></a></li><li><ol class="section"><li class="chapter-item "><a href="staking/caplin.html"><strong aria-hidden="true">6.1.</strong> Caplin</a></li><li class="chapter-item "><a href="staking/external-consensus-client-as-validator.html"><strong aria-hidden="true">6.2.</strong> Using an external consensus client as validator</a></li><li class="chapter-item "><a href="staking/shutter-network.html"><strong aria-hidden="true">6.3.</strong> Shutter Network</a></li></ol></li><li class="chapter-item expanded "><a href="monitoring/monitoring.html"><strong aria-hidden="true">7.</strong> Monitoring</a><a class="toggle"><div>❱</div></a></li><li><ol class="section"><li class="chapter-item "><a href="monitoring/creating-a-dashboard.html"><strong aria-hidden="true">7.1.</strong> Creating a dashboard</a></li><li class="chapter-item "><a href="monitoring/understanding-dashboards.html"><strong aria-hidden="true">7.2.</strong> Understanding dashboards</a></li></ol></li><li class="chapter-item expanded "><a href="tools/otterscan.html"><strong aria-hidden="true">8.</strong> Otterscan</a></li><li class="chapter-item expanded "><a href="about/about.html"><strong aria-hidden="true">9.</strong> About</a><a class="toggle"><div>❱</div></a></li><li><ol class="section"><li class="chapter-item "><a href="about/license.html"><strong aria-hidden="true">9.1.</strong> License</a></li><li class="chapter-item "><a href="about/disclaimer.html"><strong aria-hidden="true">9.2.</strong> Disclaimer</a></li><li class="chapter-item "><a href="about/donate.html"><strong aria-hidden="true">9.3.</strong> Donate</a></li><li class="chapter-item "><a href="about/how-to-reach-us.html"><strong aria-hidden="true">9.4.</strong> How to reach us</a></li></ol></li></ol>';
        // Set the current, active page, and reveal it if it's hidden
        let current_page = document.location.href.toString();
        if (current_page.endsWith("/")) {
            current_page += "index.html";
        }
        var links = Array.prototype.slice.call(this.querySelectorAll("a"));
        var l = links.length;
        for (var i = 0; i < l; ++i) {
            var link = links[i];
            var href = link.getAttribute("href");
            if (href && !href.startsWith("#") && !/^(?:[a-z+]+:)?\/\//.test(href)) {
                link.href = path_to_root + href;
            }
            // The "index" page is supposed to alias the first chapter in the book.
            if (link.href === current_page || (i === 0 && path_to_root === "" && current_page.endsWith("/index.html"))) {
                link.classList.add("active");
                var parent = link.parentElement;
                if (parent && parent.classList.contains("chapter-item")) {
                    parent.classList.add("expanded");
                }
                while (parent) {
                    if (parent.tagName === "LI" && parent.previousElementSibling) {
                        if (parent.previousElementSibling.classList.contains("chapter-item")) {
                            parent.previousElementSibling.classList.add("expanded");
                        }
                    }
                    parent = parent.parentElement;
                }
            }
        }
        // Track and set sidebar scroll position
        this.addEventListener('click', function(e) {
            if (e.target.tagName === 'A') {
                sessionStorage.setItem('sidebar-scroll', this.scrollTop);
            }
        }, { passive: true });
        var sidebarScrollTop = sessionStorage.getItem('sidebar-scroll');
        sessionStorage.removeItem('sidebar-scroll');
        if (sidebarScrollTop) {
            // preserve sidebar scroll position when navigating via links within sidebar
            this.scrollTop = sidebarScrollTop;
        } else {
            // scroll sidebar to current active section when navigating via "next/previous chapter" buttons
            var activeSection = document.querySelector('#sidebar .active');
            if (activeSection) {
                activeSection.scrollIntoView({ block: 'center' });
            }
        }
        // Toggle buttons
        var sidebarAnchorToggles = document.querySelectorAll('#sidebar a.toggle');
        function toggleSection(ev) {
            ev.currentTarget.parentElement.classList.toggle('expanded');
        }
        Array.from(sidebarAnchorToggles).forEach(function (el) {
            el.addEventListener('click', toggleSection);
        });
    }
}
window.customElements.define("mdbook-sidebar-scrollbox", MDBookSidebarScrollbox);
