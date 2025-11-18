from flask import Flask, render_template, request, redirect, url_for
from hdbcli import dbapi
from dotenv import load_dotenv
import os

load_dotenv()

app = Flask(__name__)

# SAP HANA Connection
conn = dbapi.connect(
    address=os.getenv("HANA_HOST"),
    port=int(os.getenv("HANA_PORT")),
    user=os.getenv("HANA_USER"),
    password=os.getenv("HANA_PASS")
)

SCHEMA = os.getenv("HANA_SCHEMA")
CAMPAIGN_TABLE = "ACH_FCA_CAMPAIGN"
LOOKUP_TABLE = "ACH_FCA_LOOKUP"
LOGS_TABLE = "ACH_FCA_LOGS"

# -----------------------------
# Home / Welcome screen
# -----------------------------
@app.route("/")
def home():
    return render_template("home.html")


# -----------------------------
# Campaigns Table (unchanged)
# -----------------------------
@app.route("/campaigns")
def campaigns():
    cursor = conn.cursor()
    cursor.execute(f'SELECT * FROM "{SCHEMA}"."{CAMPAIGN_TABLE}" ORDER BY CAMPAIGNID')
    rows = cursor.fetchall()
    columns = [c[0] for c in cursor.description]

    # Conditional: add RECHARGERNR and RECHARGERBR columns if RECHARGETYPE='RECHARGER'
    extended_columns = columns.copy()
    extended_rows = []
    for row in rows:
        row_list = list(row)
        if 'RECHARGETYPE' in columns:
            idx = columns.index('RECHARGETYPE')
            if row[idx] == 'RECHARGER':
                row_list.append(row[columns.index('RECHARGERNR')])
                row_list.append(row[columns.index('RECHARGERBR')])
                if 'RECHARGERNR' not in extended_columns:
                    extended_columns.extend(['RECHARGERNR', 'RECHARGERBR'])
        extended_rows.append(row_list)

    return render_template("campaigns.html", rows=extended_rows, columns=extended_columns, zip=zip)


@app.route("/campaigns/add", methods=["GET", "POST"])
def add_campaign():
    cursor = conn.cursor()

    if request.method == "POST":
        def empty_to_none(val):
            return None if val == '' else val

        # Collect all form values except TENANTID, CAMPAIGNID, CREATEDATE
        data = {}
        for key, val in request.form.items():
            if key not in ['TENANTID', 'CAMPAIGNID', 'CREATEDATE']:
                data[key] = empty_to_none(val)

        # Clear recharge fields if RECHARGETYPE != RECHARGER
        if data.get('RECHARGETYPE') != 'RECHARGER':
            data['RECHARGERNR'] = None
            data['RECHARGERBR'] = None

        # Insert into database
        columns = list(data.keys())
        values = list(data.values())
        placeholders = ','.join(['?' for _ in columns])
        insert_stmt = f'INSERT INTO "{SCHEMA}"."{CAMPAIGN_TABLE}" ({",".join(columns)}) VALUES ({placeholders})'

        cursor.execute(insert_stmt, tuple(values))
        conn.commit()
        return redirect(url_for("campaigns"))

    # GET request: fetch columns excluding dependent fields
    cursor.execute(f'SELECT * FROM "{SCHEMA}"."{CAMPAIGN_TABLE}" LIMIT 1')
    all_columns = [c[0] for c in cursor.description]

    # Exclude dependent fields from main loop
    columns = [c for c in all_columns if c not in ['BUNDLE', 'RECHARGETYPE', 'BUNDLETYPE', 'RECHARGERNR', 'RECHARGERBR']]

    return render_template("add_campaign.html", columns=columns, zip=zip)


@app.route("/campaigns/edit/<int:campaignid>", methods=["GET", "POST"])
def edit_campaign(campaignid):
    cursor = conn.cursor()

    # Editable fields
    editable_fields = [
        "CAMPAIGNNAME",
        "STARTDATE",
        "ENDDATE",
        "STATUS",
        "FCABUNDLERANGE",
        "BVSHITS_TO_FCA_RANGE",
        "IFCADATERANGE",
        "RECHARGETYPE",
        "RECHARGERNR",
        "RECHARGERBR"
    ]

    if request.method == "POST":
        def empty_to_none(val):
            return None if val == '' else val

        # Collect only editable fields
        data = {}
        for field in editable_fields:
            data[field] = empty_to_none(request.form.get(field, None))

        # If RECHARGETYPE != RECHARGER, clear recharge fields
        if data.get('RECHARGETYPE') != 'RECHARGER':
            data['RECHARGERNR'] = None
            data['RECHARGERBR'] = None

        # Build update statement dynamically
        set_clause = ', '.join([f'"{col}"=?' for col in editable_fields])
        values = [data[col] for col in editable_fields]
        values.append(campaignid)  # for WHERE clause

        cursor.execute(f'''
            UPDATE "{SCHEMA}"."{CAMPAIGN_TABLE}"
            SET {set_clause}
            WHERE CAMPAIGNID=?
        ''', tuple(values))
        conn.commit()

        return redirect(url_for("campaigns"))

    # GET request: fetch existing campaign
    cursor.execute(f'SELECT * FROM "{SCHEMA}"."{CAMPAIGN_TABLE}" WHERE CAMPAIGNID=?', (campaignid,))
    campaign = cursor.fetchone()
    columns = [c[0] for c in cursor.description]
    
    # Convert row to dictionary for easier access in template
    campaign_dict = dict(zip(columns, campaign))

    return render_template("edit_campaign.html", campaign=campaign_dict, columns=columns, zip=zip)


@app.route("/campaigns/delete/<int:campaignid>")
def delete_campaign(campaignid):
    cursor = conn.cursor()
    cursor.execute(f'DELETE FROM "{SCHEMA}"."{CAMPAIGN_TABLE}" WHERE CAMPAIGNID=?', (campaignid,))
    conn.commit()
    return redirect(url_for("campaigns"))


# -----------------------------
# Lookup Table
# -----------------------------
@app.route("/lookup")
def lookup():
    cursor = conn.cursor()
    cursor.execute(f'SELECT * FROM "{SCHEMA}"."{LOOKUP_TABLE}" ORDER BY CAMPAIGNID')
    rows = cursor.fetchall()
    columns = [c[0] for c in cursor.description]
    return render_template("lookup.html", rows=rows, columns=columns, zip=zip)


@app.route("/lookup/add", methods=["GET", "POST"])
def add_lookup():
    if request.method == "POST":
        campaignid = request.form["CAMPAIGNID"]
        retailerid = request.form["RETAILERID"]
        productid = request.form["PRODUCTID"]
        startdate = request.form["STARTDATE"]
        enddate = request.form["ENDDATE"]
        target = request.form["TARGET"]
        commission = request.form["COMMISSION"]
        min_val = request.form["MIN"]
        max_val = request.form["MAX"]
        cap = request.form["CAP"]

        cursor = conn.cursor()
        cursor.execute(f'''
            INSERT INTO "{SCHEMA}"."{LOOKUP_TABLE}"
            (CAMPAIGNID, RETAILERID, PRODUCTID, STARTDATE, ENDDATE, TARGET, COMMISSION, MIN, MAX, CAP)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (campaignid, retailerid, productid, startdate, enddate, target, commission, min_val, max_val, cap))
        conn.commit()
        return redirect(url_for("lookup"))
    return render_template("add_lookup.html")

@app.route("/lookup/edit/<int:campaignid>/<retailerid>/<productid>", methods=["GET", "POST"])
def edit_lookup(campaignid, retailerid, productid):
    cursor = conn.cursor()

    # Helper function to convert empty strings to None for numeric columns
    def empty_to_none(value):
        return None if value == '' else value

    if request.method == "POST":
        # Get form values
        tenantid = request.form.get("TENANTID")  # read-only, but can still include if needed
        new_campaignid = request.form.get("CAMPAIGNID")  # editable
        startdate = request.form.get("STARTDATE")
        enddate = request.form.get("ENDDATE")
        target = empty_to_none(request.form.get("TARGET"))
        commission = empty_to_none(request.form.get("COMMISSION"))
        min_val = empty_to_none(request.form.get("MIN"))
        max_val = empty_to_none(request.form.get("MAX"))
        cap = empty_to_none(request.form.get("CAP"))

        # Update the lookup table; update CAMPAIGNID as well
        cursor.execute(f'''
            UPDATE "{SCHEMA}"."{LOOKUP_TABLE}"
            SET CAMPAIGNID=?, STARTDATE=?, ENDDATE=?, TARGET=?, COMMISSION=?, MIN=?, MAX=?, CAP=?, MODIFICATIONDATE=CURRENT_TIMESTAMP
            WHERE CAMPAIGNID=? AND RETAILERID=? AND PRODUCTID=?
        ''', (new_campaignid, startdate, enddate, target, commission, min_val, max_val, cap, campaignid, retailerid, productid))
        conn.commit()

        return redirect(url_for("lookup"))

    # GET request: fetch the existing row
    cursor.execute(f'''
        SELECT * FROM "{SCHEMA}"."{LOOKUP_TABLE}"
        WHERE CAMPAIGNID=? AND RETAILERID=? AND PRODUCTID=?
    ''', (campaignid, retailerid, productid))
    lookup_row = cursor.fetchone()
    columns = [c[0] for c in cursor.description]

    return render_template("edit_lookup.html", lookup=lookup_row, columns=columns, zip=zip)



@app.route("/lookup/delete/<int:campaignid>/<retailerid>/<productid>")
def delete_lookup(campaignid, retailerid, productid):
    cursor = conn.cursor()
    cursor.execute(f'''
        DELETE FROM "{SCHEMA}"."{LOOKUP_TABLE}" 
        WHERE CAMPAIGNID=? AND RETAILERID=? AND PRODUCTID=?
    ''', (campaignid, retailerid, productid))
    conn.commit()
    return redirect(url_for("lookup"))


# -----------------------------
# Logs Table
# -----------------------------
@app.route("/logs")
def logs():
    cursor = conn.cursor()
    cursor.execute(f'SELECT * FROM "{SCHEMA}"."{LOGS_TABLE}" ORDER BY COMPENSATIONDATE DESC')
    rows = cursor.fetchall()
    columns = [c[0] for c in cursor.description]
    return render_template("logs.html", rows=rows, columns=columns, zip=zip)


# -----------------------------
# Run server
# -----------------------------
if __name__ == "__main__":
    app.run(debug=True)
