"""
Minimal Flask web app demonstrating the custom DB engine.

Shows how the database can be integrated into a web application.
"""

from flask import Flask, render_template, request, jsonify, redirect, url_for
import sys
import os

# Add parent directory to path to import db module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from db.engine import DatabaseEngine
from db.column import Column, DataType  # FIXED: Import Column and DataType
from db.parser import QueryParser


app = Flask(__name__)

# Initialize database
db = DatabaseEngine()

# Create sample table if it doesn't exist
try:
    db.create_table("users", [
        Column("id", DataType.INT, is_primary_key=True),  # FIXED: Use Column directly
        Column("email", DataType.TEXT, is_unique=True),   # FIXED: Use Column directly
        Column("name", DataType.TEXT)                     # FIXED: Use Column directly
    ])
    
    # Add some sample data
    db.insert_into("users", [1, "alice@example.com", "Alice Johnson"])
    db.insert_into("users", [2, "bob@example.com", "Bob Smith"])
    db.insert_into("users", [3, "charlie@example.com", "Charlie Brown"])
except ValueError:
    # Table already exists
    pass


@app.route('/')
def index():
    """Home page with database info."""
    tables = db.get_table_info()
    return render_template('index.html', tables=tables)


@app.route('/users')
def list_users():
    """List all users."""
    try:
        users = db.select_from("users")
        return render_template('users.html', users=users)
    except Exception as e:
        return f"Error: {e}", 500


@app.route('/add-user', methods=['GET', 'POST'])
def add_user():
    """Add a new user."""
    if request.method == 'POST':
        try:
            # Get form data
            user_id = int(request.form['id'])
            email = request.form['email']
            name = request.form['name']
            
            # Insert into database
            db.insert_into("users", [user_id, email, name])
            
            return redirect(url_for('list_users'))
        except ValueError as e:
            return f"Error: {e}", 400
        except Exception as e:
            return f"Error: {e}", 500
    
    return render_template('add_user.html')


@app.route('/delete-user/<int:user_id>')
def delete_user(user_id):
    """Delete a user by ID."""
    try:
        db.delete_from("users", {"id": user_id})
        return redirect(url_for('list_users'))
    except Exception as e:
        return f"Error: {e}", 500


@app.route('/update-user/<int:user_id>', methods=['GET', 'POST'])
def update_user(user_id):
    """Update a user."""
    if request.method == 'POST':
        try:
            email = request.form['email']
            name = request.form['name']
            
            db.update_table("users", 
                           {"email": email, "name": name},
                           {"id": user_id})
            
            return redirect(url_for('list_users'))
        except Exception as e:
            return f"Error: {e}", 500
    
    # GET - show current data
    try:
        users = db.select_from("users", where={"id": user_id})
        if not users:
            return "User not found", 404
        
        user = users[0]
        return render_template('update_user.html', user=user)
    except Exception as e:
        return f"Error: {e}", 500


@app.route('/api/query', methods=['POST'])
def execute_query():
    """Execute raw SQL query (API endpoint)."""
    try:
        query = request.json.get('query', '')
        if not query:
            return jsonify({"error": "No query provided"}), 400
        
        # Parse and execute query
        parsed = QueryParser.parse_query(query)
        
        # Manual execution for API
        cmd_type = parsed["type"]
        
        if cmd_type == "CREATE_TABLE":
            db.create_table(parsed["table_name"], parsed["columns"])
            result = {"message": f"Table created: {parsed['table_name']}"}
        
        elif cmd_type == "INSERT":
            db.insert_into(parsed["table_name"], parsed["values"])
            result = {"message": "Row inserted", "count": 1}
        
        elif cmd_type == "SELECT":
            rows = db.select_from(
                parsed["table_name"],
                parsed["columns"],
                parsed["where"]
            )
            result = {"rows": rows, "count": len(rows)}
        
        elif cmd_type == "UPDATE":
            count = db.update_table(
                parsed["table_name"],
                parsed["updates"],
                parsed["where"]
            )
            result = {"message": "Rows updated", "count": count}
        
        elif cmd_type == "DELETE":
            count = db.delete_from(
                parsed["table_name"],
                parsed["where"]
            )
            result = {"message": "Rows deleted", "count": count}
        
        else:
            return jsonify({"error": f"Unknown command: {cmd_type}"}), 400
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route('/api/tables')
def list_tables_api():
    """API endpoint to list tables."""
    try:
        tables = db.get_table_info()
        return jsonify({"tables": tables})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Create templates directory
TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), 'templates')
os.makedirs(TEMPLATES_DIR, exist_ok=True)

# Create simple HTML templates
with open(os.path.join(TEMPLATES_DIR, 'index.html'), 'w') as f:
    f.write("""<!DOCTYPE html>
<html>
<head>
    <title>Mini RDBMS Web Demo</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }
        .container { background: #f5f5f5; padding: 20px; border-radius: 5px; }
        h1 { color: #333; }
        .menu { margin: 20px 0; }
        .menu a { display: inline-block; margin-right: 15px; padding: 10px 15px; 
                  background: #4CAF50; color: white; text-decoration: none; border-radius: 3px; }
        .menu a:hover { background: #45a049; }
        .table { background: white; border-radius: 5px; padding: 15px; margin-top: 20px; }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }
        th { background-color: #f2f2f2; }
        .api-section { margin-top: 30px; }
        .api-section textarea { width: 100%; height: 100px; }
        .api-section button { padding: 10px 20px; background: #008CBA; color: white; border: none; cursor: pointer; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Mini RDBMS Web Demo</h1>
        <p>This is a demonstration of the custom in-memory database engine.</p>
        
        <div class="menu">
            <a href="/users">View Users</a>
            <a href="/add-user">Add User</a>
        </div>
        
        <div class="table">
            <h2>Database Status</h2>
            {% if tables %}
                <p>Tables in database: {{ tables|length }}</p>
                <ul>
                {% for table_name, info in tables.items() %}
                    <li>{{ table_name }}: {{ info.row_count }} rows</li>
                {% endfor %}
                </ul>
            {% else %}
                <p>No tables in database.</p>
            {% endif %}
        </div>
        
        <div class="api-section">
            <h2>Raw SQL Query</h2>
            <textarea id="query" placeholder="Enter SQL query..."></textarea>
            <br>
            <button onclick="executeQuery()">Execute</button>
            <div id="result" style="margin-top: 15px;"></div>
        </div>
    </div>
    
    <script>
    function executeQuery() {
        const query = document.getElementById('query').value;
        const resultDiv = document.getElementById('result');
        
        fetch('/api/query', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({query: query})
        })
        .then(response => response.json())
        .then(data => {
            if (data.error) {
                resultDiv.innerHTML = `<div style="color: red;">Error: ${data.error}</div>`;
            } else {
                resultDiv.innerHTML = `<div style="color: green;">Success: ${JSON.stringify(data)}</div>`;
            }
        })
        .catch(error => {
            resultDiv.innerHTML = `<div style="color: red;">Error: ${error}</div>`;
        });
    }
    </script>
</body>
</html>""")

with open(os.path.join(TEMPLATES_DIR, 'users.html'), 'w') as f:
    f.write("""<!DOCTYPE html>
<html>
<head>
    <title>Users - Mini RDBMS</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 1000px; margin: 0 auto; padding: 20px; }
        h1 { color: #333; }
        .menu { margin: 20px 0; }
        .menu a { display: inline-block; margin-right: 15px; padding: 10px 15px; 
                  background: #4CAF50; color: white; text-decoration: none; border-radius: 3px; }
        .menu a:hover { background: #45a049; }
        table { width: 100%; border-collapse: collapse; margin-top: 20px; }
        th, td { padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }
        th { background-color: #f2f2f2; }
        tr:hover { background-color: #f5f5f5; }
        .actions a { margin-right: 10px; color: #2196F3; text-decoration: none; }
        .actions a:hover { text-decoration: underline; }
        .delete { color: #f44336 !important; }
    </style>
</head>
<body>
    <h1>Users</h1>
    
    <div class="menu">
        <a href="/">Home</a>
        <a href="/add-user">Add New User</a>
    </div>
    
    <table>
        <thead>
            <tr>
                <th>ID</th>
                <th>Email</th>
                <th>Name</th>
                <th>Actions</th>
            </tr>
        </thead>
        <tbody>
            {% for user in users %}
            <tr>
                <td>{{ user.id }}</td>
                <td>{{ user.email }}</td>
                <td>{{ user.name }}</td>
                <td class="actions">
                    <a href="/update-user/{{ user.id }}">Edit</a>
                    <a href="/delete-user/{{ user.id }}" class="delete" 
                       onclick="return confirm('Delete user {{ user.name }}?')">Delete</a>
                </td>
            </tr>
            {% endfor %}
        </tbody>
    </table>
    
    <p>Total: {{ users|length }} user(s)</p>
</body>
</html>""")

with open(os.path.join(TEMPLATES_DIR, 'add_user.html'), 'w') as f:
    f.write("""<!DOCTYPE html>
<html>
<head>
    <title>Add User - Mini RDBMS</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; }
        h1 { color: #333; }
        .menu { margin: 20px 0; }
        .menu a { display: inline-block; margin-right: 15px; padding: 10px 15px; 
                  background: #4CAF50; color: white; text-decoration: none; border-radius: 3px; }
        .menu a:hover { background: #45a049; }
        .form-group { margin-bottom: 15px; }
        label { display: block; margin-bottom: 5px; font-weight: bold; }
        input[type="text"], input[type="number"] { 
            width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 3px; 
        }
        button { padding: 10px 20px; background: #4CAF50; color: white; border: none; cursor: pointer; }
        button:hover { background: #45a049; }
    </style>
</head>
<body>
    <h1>Add New User</h1>
    
    <div class="menu">
        <a href="/">Home</a>
        <a href="/users">Back to Users</a>
    </div>
    
    <form method="POST">
        <div class="form-group">
            <label for="id">ID (Primary Key):</label>
            <input type="number" id="id" name="id" required>
        </div>
        
        <div class="form-group">
            <label for="email">Email (Unique):</label>
            <input type="text" id="email" name="email" required>
        </div>
        
        <div class="form-group">
            <label for="name">Name:</label>
            <input type="text" id="name" name="name" required>
        </div>
        
        <button type="submit">Add User</button>
    </form>
</body>
</html>""")

with open(os.path.join(TEMPLATES_DIR, 'update_user.html'), 'w') as f:
    f.write("""<!DOCTYPE html>
<html>
<head>
    <title>Update User - Mini RDBMS</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; }
        h1 { color: #333; }
        .menu { margin: 20px 0; }
        .menu a { display: inline-block; margin-right: 15px; padding: 10px 15px; 
                  background: #4CAF50; color: white; text-decoration: none; border-radius: 3px; }
        .menu a:hover { background: #45a049; }
        .form-group { margin-bottom: 15px; }
        label { display: block; margin-bottom: 5px; font-weight: bold; }
        input[type="text"] { 
            width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 3px; 
        }
        button { padding: 10px 20px; background: #4CAF50; color: white; border: none; cursor: pointer; }
        button:hover { background: #45a049; }
        .info { background: #f0f0f0; padding: 10px; border-radius: 3px; margin-bottom: 20px; }
    </style>
</head>
<body>
    <h1>Update User</h1>
    
    <div class="menu">
        <a href="/">Home</a>
        <a href="/users">Back to Users</a>
    </div>
    
    <div class="info">
        Updating user ID: {{ user.id }}
    </div>
    
    <form method="POST">
        <div class="form-group">
            <label for="email">Email (Unique):</label>
            <input type="text" id="email" name="email" value="{{ user.email }}" required>
        </div>
        
        <div class="form-group">
            <label for="name">Name:</label>
            <input type="text" id="name" name="name" value="{{ user.name }}" required>
        </div>
        
        <button type="submit">Update User</button>
    </form>
</body>
</html>""")

if __name__ == '__main__':
    print("Starting Mini RDBMS Web Demo...")
    print("Open http://localhost:5000 in your browser")
    app.run(debug=True, port=5000)