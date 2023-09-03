const loginForm = document.getElementById('login-form');
const filterForm = document.getElementById('filter-form');
// Get references to the objects
var loginButton = document.getElementById("loginButton");
var submitButton = document.getElementById("submitButton");
var downloadButton = document.getElementById("downloadButton");


function deletePreviousTable() {
    var tables = document.getElementsByTagName("table");

    if (tables.length > 0) {
      // remove the first table element from the DOM
      tables[0].parentNode.removeChild(tables[0]);
      console.log("The first table has been deleted from the document.");
    } else {
      console.log("There are no tables in the document.");
    }
  }
loginButton.addEventListener('click', (event) => {
//form.addEventListener('submit', (event) => {
    event.preventDefault(); // Prevent form submission
    // Hide login form, show data input form
    loginForm.style.display = "none";
    filterForm.style.display = "block";

    const formData = new FormData(loginForm); // Get form data
    const username = formData.get('username');
    const password = formData.get('password');
    //alert(username);

    // Send data to Flask endpoint
    fetch('/login', {
        method: 'POST',
        body: JSON.stringify({username, password}),
        headers: {
            'Content-Type': 'application/json'
        }
    })
/*
    .then(response => response.json())
    //.then(response => alert(response))
    //.then(data => console.log(data.message))
    .then(data => alert(data.message))
    //.catch(error => console.error(error));
    .catch(error => alert(error));
*/    
    .then(response => response.json())
    .then(data => {
    console.log(data); // log table data to console

    deletePreviousTable();
    // create table element and table header row
    const table = document.createElement('table');
    const headerRow = table.insertRow();
    const headers = ['col1', 'col2', 'col3'];

    // create table header cells
    headers.forEach(header => {
        const cell = headerRow.insertCell();
        cell.innerHTML = header;
    });

    // fill table with data
    data.forEach(rowData => {
        const row = table.insertRow();
        headers.forEach(header => {
        const cell = row.insertCell();
        cell.innerHTML = rowData[header];
        });
    });

    // add table to document body
    document.body.appendChild(table);
    })
    .catch(error => console.error(error));

});


submitButton.addEventListener('click', (event) => {
    //form.addEventListener('submit', (event) => {
        event.preventDefault(); // Prevent form submission
        // Hide login form, show data input form
        loginForm.style.display = "none";
        filterForm.style.display = "block";
    
        const formData = new FormData(filterForm); // Get form data
        const msisdn_nsk = formData.get('msisdn_nsk');
        const actual_apn_v = formData.get('actual_apn_v');
        const economic_code_n = formData.get('economic_code_n');
        const kit_number_v = formData.get('kit_number_v');        
        //alert(username);
    
        // Send data to Flask endpoint
        fetch('/submit', {
            method: 'POST',
            body: JSON.stringify({msisdn_nsk, actual_apn_v, economic_code_n, kit_number_v}),
            headers: {
                'Content-Type': 'application/json'
            }
        })
    /*
        .then(response => response.json())
        //.then(response => alert(response))
        //.then(data => console.log(data.message))
        .then(data => alert(data.message))
        //.catch(error => console.error(error));
        .catch(error => alert(error));
    */    
        .then(response => response.json())
        .then(data => {
            // Display the message in the message-container div
            const messageContainer = document.getElementById('message-container');
            messageContainer.textContent = data.message;
        })
        .catch(error => console.error(error));
    
    });
    


downloadButton.addEventListener('click', async (event) => {
    event.preventDefault(); // Prevent default link behavior
    try {
        const response = await fetch('/download_excel', {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json'
            }
        });

        if (response.ok) {
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'query_result.xlsx'; // Set desired filename
            document.body.appendChild(a);
            a.click();
            a.remove();
        } else {
            console.error('Download failed.');
        }
    } catch (error) {
        console.error('Error:', error);
    }
});

