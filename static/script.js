const form = document.getElementById('login-form');
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
form.addEventListener('submit', (event) => {
    event.preventDefault(); // Prevent form submission

    const formData = new FormData(form); // Get form data
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
