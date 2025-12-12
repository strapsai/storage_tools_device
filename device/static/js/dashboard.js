const configFields = ["watch", "servers", "include_suffix"];


document.addEventListener("DOMContentLoaded", function() {
    const timezoneSelect = document.getElementById("local_tz");
    const timezones = Intl.supportedValuesOf("timeZone");  // Supported time zones
    
    // Populate the dropdown with time zones
    timezones.forEach(tz => {
        const option = document.createElement("option");
        option.value = tz;
        option.textContent = tz;
        timezoneSelect.appendChild(option);
    });
});

// Function to add list items dynamically
function addListItem(field) {
    const newItem = document.getElementById(`new-${field}-item`).value.trim();
    if (newItem) {
        const listElement = document.getElementById(`${field}-list`);
        const itemElement = document.createElement("div");
        itemElement.className = "input-group mb-1";
        itemElement.innerHTML = `
            <input type="text" class="form-control" value="${newItem}" readonly>
            <button class="btn btn-danger" onclick="removeListItem(this)">&#128465;</button>
        `;
        listElement.appendChild(itemElement);
        document.getElementById(`new-${field}-item`).value = ""; // Clear input field
    }
}

// Function to remove list items
function removeListItem(element) {
    element.parentElement.remove();
}

// Function to save configuration
function saveConfig() {
    const config = {
        project: document.getElementById("project_name").value,
        robot_name: document.getElementById("robot_name").value,
        API_KEY_TOKEN: document.getElementById("API_KEY_TOKEN").value,
        watch: getListItems('watch'),
        local_tz: document.getElementById("local_tz").value,
        servers: getListItems('servers'),
        threads: parseInt(document.getElementById("threads").value),
        include_suffix: getListItems('include_suffix'),
        wait_s: parseInt(document.getElementById("wait_s").value)
    };

    fetch("/save_config", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(config)
    }).then(response => {
        if (response.ok) {
            alert("Configuration saved successfully!");
        } else {
            alert("Failed to save configuration.");
        }
    });
}

// Function to get list items
function getListItems(field) {
    const items = [];
    document.getElementById(`${field}-list`).querySelectorAll('input').forEach(input => {
        items.push(input.value);
    });
    return items;
}

// Function to refresh configuration
function refreshConfig() {
    fetch("/get_config")
        .then(response => response.json())
        .then(config => {
            document.getElementById("project_name").value = config.project || "";
            document.getElementById("robot_name").value = config.robot_name || "";
            document.getElementById("API_KEY_TOKEN").value = config.API_KEY_TOKEN || "";
            document.getElementById("local_tz").value = config.local_tz || "";
            document.getElementById("threads").value = config.threads || 4;
            document.getElementById("wait_s").value = config.wait_s || 2;
            document.getElementById("split_size_gb").value = config.split_size_gb || 2;
            document.getElementById("read_size_mb").value = config.read_size_mb || 2;

            // Populate list fields
            configFields.forEach(field => {
                console.log(field, `${field}-list`);
                const listElement = document.getElementById(`${field}-list`);
                listElement.innerHTML = '';  // Clear existing items
                (config[field] || []).forEach(item => addListItemToField(field, item));
            });
        });
}

function formatBytes(bytes) {
    if (!bytes && bytes !== 0) return "--";
    let size = Number(bytes);
    if (Number.isNaN(size)) return "--";
    const units = ["B", "KB", "MB", "GB", "TB"];
    let unit = 0;
    while (size >= 1024 && unit < units.length - 1) {
        size /= 1024;
        unit += 1;
    }
    return `${size.toFixed(unit === 0 ? 0 : 1)} ${units[unit]}`;
}

function formatTime(value) {
    return value && value.length > 0 ? value : "--";
}

function renderFiles(payload) {
    const tbody = document.getElementById("files-tbody");
    const meta = document.getElementById("files-meta");
    if (!tbody) return;

    tbody.innerHTML = "";
    const files = payload?.files || [];

    if (meta) {
        const scanningNote = payload?.scanning ? " (scan running)" : "";
        meta.textContent = `Showing ${files.length} file${files.length === 1 ? "" : "s"}${scanningNote}`;
    }

    if (files.length === 0) {
        const row = document.createElement("tr");
        row.innerHTML = `<td colspan="5" class="text-muted">No files yet${payload?.scanning ? " (scan running...)" : ""}</td>`;
        tbody.appendChild(row);
        return;
    }

    files.forEach(file => {
        const tr = document.createElement("tr");
        const filepath = [file.dirroot, file.filename].filter(Boolean).join("/");
        tr.innerHTML = `
            <td class="text-break">${filepath}</td>
            <td class="text-end">${formatBytes(file.size)}</td>
            <td>${formatTime(file.start_time)}</td>
            <td>${formatTime(file.end_time)}</td>
            <td>${file.site || "--"}</td>
        `;
        tbody.appendChild(tr);
    });
}

function refreshFiles() {
    const meta = document.getElementById("files-meta");
    if (meta) meta.textContent = "Loading files...";

    fetch("/get_files")
        .then(response => {
            if (!response.ok) {
                throw new Error(`Failed to fetch files (${response.status})`);
            }
            return response.json();
        })
        .then(renderFiles)
        .catch(err => {
            console.error(err);
            if (meta) meta.textContent = "Failed to load files";
            const tbody = document.getElementById("files-tbody");
            if (tbody) {
                tbody.innerHTML = '<tr><td colspan="5" class="text-danger">Unable to fetch files</td></tr>';
            }
        });
}

// Helper function to add list item from server data
function addListItemToField(field, value) {
    const listElement = document.getElementById(`${field}-list`);
    const itemElement = document.createElement("div");
    itemElement.className = "input-group mb-1";
    itemElement.innerHTML = `
        <input type="text" class="form-control" value="${value}" readonly>
        <button class="btn btn-danger" onclick="removeListItem(this)">&#128465;</button>
    `;
    listElement.appendChild(itemElement);
}


function disconnect() {
    fetch("/disconnect")
}

// Refresh config on page load
document.addEventListener('DOMContentLoaded', refreshConfig);
document.addEventListener('DOMContentLoaded', () => {
    // Preload files list on initial page load
    refreshFiles();

    // Refresh when the Connection tab becomes active
    const connectionTab = document.querySelector('a.nav-link[href="#connection-tab"]');
    if (connectionTab) {
        connectionTab.addEventListener('shown.bs.tab', refreshFiles);
    }
});


// connect to web socket to get connection status. 

$(document).ready(function () {

    let protocol = window.location.protocol === 'https:' ? 'https:' : 'http:';
    let url = protocol + "//" + document.domain + ':' + location.port;

    var socket = io.connect(url, { transports: ['websocket']});


    socket.on("connect", function(){
        console.log("Connected to device")
        let deviceStatusDiv = document.getElementById('device_connection_status');
        deviceStatusDiv.className = "connection_status_online";
        deviceStatusDiv.textContent = "Device: Online"
    
    });

    socket.on("disconnect", function() {
        console.log("Disconnected from device")
        let deviceStatusDiv = document.getElementById('device_connection_status');
        deviceStatusDiv.className = "connection_status_offline";
        deviceStatusDiv.textContent = "Device: Offline";

        let serverStatusDiv = document.getElementById('server_connection_status');
        serverStatusDiv.className = "connection_status_unknown";
        serverStatusDiv.textContent = "Server: Unknown";
    });
    

    socket.on("server_connect", function(msg) {
        let serverStatusDiv = document.getElementById('server_connection_status');
        serverStatusDiv.className = "connection_status_online";
        serverStatusDiv.textContent = "Server: Online";

        // let nameInput = document.getElementById("connection_server_name")
        // nameInput.value = msg.name 

        window.serverStatus[msg.name] = msg.connected;
        
        updateConnections();

    });

    socket.on("server_remove", function(msg) {
        server_address = msg.name;

        if( window.serverStatus[server_address]) {
            delete window.serverStatus[server_address];
        }

        updateConnections()
    })

    socket.on("server_connections", function(msg) {
        window.serverStatus  = {}

        $.each(msg, server_address => {
            window.serverStatus[server_address] = msg[server_address];
        })
        updateConnections()
    })

    socket.on("title", function(title) {
        const div = document.getElementById("title")
        div.innerHTML = title;
        document.title = "Dev Config " + title 
    })

    socket.on("version", function(version) {
        document.querySelectorAll(".version_number").forEach( span => {
            console.log(span)
            span.innerText = version
        })
    })

    socket.on('device_status_tqdm', function (msg) {
        //console.log(msg)
        updateProgress(msg, 'device-status-tqdm');
      });

    socket.on("ping", function(msg) {
        console.log(msg);
    })

    socket.on("status", function(msg) {
        const status = document.getElementById("status")
        if(status) {
            status.value += msg.msg + "\n"
        }
    })
    
});


function updateConnections() {
    container = document.getElementById("connection_list")
    container.innerHTML = ""

    connections = Object.entries(window.serverStatus).sort();
    any_connected = false;

    connections.forEach( element  => {
        let connection_name = element[0];
        let connected = element[1][0];
        let source = element[1][1];

        let id = "status_" + connection_name.replaceAll(".", "_").replaceAll(":", "_")
        status_item = document.getElementById(id)
        if( status_item == null ) {
            status_item = document.createElement("div")
            container.appendChild(status_item)
        }
        status_item.innerHTML = "";
        
        let icon = document.createElement("i")
        status_item.appendChild(icon)

        let span = document.createElement("span")
        status_item.appendChild(span)
        
        if(connected) {
            span.className = "connection_list_online"
            icon.className = "bi bi-cloud-fill"
            span.innerHTML = connection_name + " " + source + " Connected"

            any_connected = true;
        } else {
            span.className = "connection_list_offline"
            span.innerHTML = connection_name

            icon.className = "bi bi-cloud"
        }

    })

    let serverStatusDiv = document.getElementById('server_connection_status');
    if( any_connected) {
    serverStatusDiv.className = "connection_status_online";
    serverStatusDiv.textContent = "Server: Online";
    } else {
        serverStatusDiv.className = "connection_status_offline";
        serverStatusDiv.textContent = "Server: Disconnected";
    }
    // $.each(connections, function( name, connected) {
    //     console.log(name, connected)
    // })
}

function debugSocket() {
    fetch("debug")
}

function refresh() {
    fetch("refresh")
}

function restartConnections() {
    fetch("restartConnections")
}

function emitFiles()
{
    fetch("emitFiles")
}

function scan()
{
    fetch("scan")
}

window.serverStatus = {}