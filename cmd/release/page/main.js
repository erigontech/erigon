fetch("./data.json")
  .then((response) => response.json())
  .then((data) => {
    const osList = ["linux", "darwin", "windows", "docker"];
    const releases = data.releases;

    const osData = osList.map((os) => {
      const data = releases.map((release) => {
        const binaries = release.binaries.filter((binary) => {
          return binary.os === os;
        });

        return {
          version: release.version,
          os: os,
          binaries,
        };
      });

      if (os === "darwin") {
        os = "macOS";
      }

      return {
        os: os,
        stable: data.filter((release) => !release.version.includes("rc")),
        rc: data.filter((release) => release.version.includes("rc")),
      };
    });

    const stableTabContainer = document.getElementById("tab-container");
    const rcTabContainer = document.getElementById("rc-tab-container");

    const stableContainer = document.getElementById("stable-data");
    const stableNoData = document.getElementById("stable-no-data");

    const rcContainer = document.getElementById("rc-data");
    const rcNoData = document.getElementById("rc-no-data");

    if (osData) {
      let stableLength = 0;
      let rcLength = 0;

      osData.map((os) => {
        stableLength += os.stable.length;
        rcLength += os.rc.length;
      });

      if (stableLength === 0) {
        stableContainer.style.display = "none";
        stableNoData.style.display = "block";
      }

      if (rcLength === 0) {
        rcContainer.style.display = "none";
        rcNoData.style.display = "block";
      }
    }

    osData.map((os, idx) => {
      const tabContent = document.createElement("div");
      tabContent.setAttribute("id", os.os);
      tabContent.classList.add("tabcontent");
      tabContent.setAttribute("data-type", "stable");

      if (idx === 0) {
        tabContent.classList.add("active");
      }

      let eventIdsToAdd = [];

      const html = os.stable.map((release, index) => {
        eventIdsToAdd.push(`${release.version}-${os.os}`);
          let cls = "panel";
          if (index === 0) {
              cls += " panel-active";
          }

          let als = "accordion";
            if (index === 0) {
                als += " accordion-active";
            }

        return `
          <button id="${release.version}-${os.os}" class="${als}">
            ${
              os.os !== "macOS"
                ? os.os.charAt(0).toUpperCase() + os.os.slice(1)
                : os.os
            } - ${release.version} 
    
            <span class="accordion-icon">
              <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="feather feather-chevron-down"><polyline points="6 9 12 15 18 9"></polyline></svg>
            </span>
          </button>
            <div class="${cls}" id="${release.version}-${os.os}-panel">
              <table class="table">
                <thead class="table-head">
                  <tr>
                    <th class="table-head-col" style="padding-left:18px;">Binary</th>
                    <th class="table-head-col">Arch</th>
                    <th class="table-head-col">File</th>              
                    <th class="table-head-col">Checksum</th>
                    <th class="table-head-col">Tag</th>
                  </tr>
                </thead>
                <tbody>
                ${
                    release.binaries.length === 0
                    ? `
                        <tr class="table-row">
                            <td class="table-col" style="padding-left:18px;">No binaries available</td>
                        </tr>
                    `
                    : ""
                }
                
                ${release.binaries
                  .map((binary) => {
                    // create horizontal html table in the return
                    return `
                      <tr class="table-row">
                        <td class="table-col" style="padding-left:18px;">${binary.name}</td>
                        <td class="table-col">${binary.arch}</td>
                        <td class="table-col"><a href="${binary.file}" target="_blank">${binary.file_name}</a></td>
                        <td class="table-col">${binary.checksum}</td>
                        <td class="table-col">${binary.tag}</td>
                      </tr>
                  `;
                  })
                  .join("")}
                </tbody>
              </table>
            </div>
        `;
      });

      tabContent.innerHTML = html.join("");
      stableTabContainer.appendChild(tabContent);

      eventIdsToAdd.forEach((id) => {
        const element = document.getElementById(`${id}`);

        if (element) {
          element.addEventListener("click", () => {
            openAccordion(id);
          });
        }
      });

      eventIdsToAdd = [];
    });

    function openAccordion(accordionId) {
      const panel = document.getElementById(accordionId + "-panel");
      const accordion = document.getElementById(accordionId);

      if (accordion.classList.contains("accordion-active")) {
        accordion.classList.remove("accordion-active");
      } else {
        accordion.classList.add("accordion-active");
      }

      if (panel.classList.contains("panel-active")) {
        panel.classList.remove("panel-active");
      } else {
        panel.classList.add("panel-active");
      }
    }
  });

function openTab(evt, tabId, dataType) {
    // Declare all variables
    let i, tabcontent, tablinks;

    // Get all elements with dataType attribute and hide them
    tabcontent = document.querySelectorAll(`[data-type=${dataType}]`);
    for (i = 0; i < tabcontent.length; i++) {
        tabcontent[i].style.display = "none";
    }

    // Get all elements with class="tablinks" and remove the class "active"
    tablinks = document.getElementsByClassName(`tablinks-${dataType}`);
    for (i = 0; i < tablinks.length; i++) {
        tablinks[i].className = tablinks[i].className.replace("active", "");
    }

    // Show the current tab, and add an "active" class to the button that opened the tab
    document.getElementById(tabId).style.display = "block";
    evt.currentTarget.className += " active";
}
