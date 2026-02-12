PREMAV Data Sync & Automated ETL Pipeline
Project Description
This project automates the extraction, transformation, and loading (ETL) of research data from REDCap into a MySQL database. It is designed to bridge the gap between raw data collection and real-time analysis by ensuring that the database is always synchronized with the most current, cleaned information.
🚀 Key Features
* Automated Batch Fetching: Successfully bypasses REDCap API memory and timeout limitations (specifically the 336-record wall) by requesting data in manageable chunks.
* Intelligent Data Cleaning:
o Automatically handles longitudinal data by forward-filling and back-filling demographic and static fields across repeating events.
o Filters incomplete records to ensure data integrity.
* Custom Row Indexing: Replaces internal REDCap identifiers with a clean, sequential range ($1$ to $n$) while maintaining record_id as a primary key for database consistency.
* Analysis-Ready Output: Generates a cleaned, processed dataset in both MySQL and Excel (.xlsx) formats, with calculated status fields ready for immediate statistical analysis.
* Dynamic Schema Mapping: Automatically creates and updates MySQL tables based on the first 256 columns of the processed DataFrame.

Technical Architecture
* Source: REDCap API (Raw Data)
* Processing: Python (Pandas & NumPy)
* Storage:
o Relational: MySQL (Local or Remote)
o Flat File: Excel (For manual review/backup)

File Structure
* script.py: The execution engine. It manages the infinite loop, handles API communication via batching, and triggers the sync every 30 seconds.
* premav.py: The logic module. Contains the data cleaning algorithms, demographic filling, sequential ID generation, and the dynamic MySQL REPLACE INTO logic.

Data Transformation Logic
To ensure the dataset is ready for analysis, the following transformations are applied:
1. Longitudinal Filling: Demographic data is synchronized across all repeating instances for a single record.
2. Participant Status Assignment: A status column is dynamically created by parsing participant_id strings (categorizing as Recovered, Contact, or Control).
3. Column Constraint: Data is limited to the first 256 variables to ensure compatibility with standard database row-size limits.
4. Range Indexing: The record_id is transformed into a clean integer sequence.

Setup & Installation
1. Clone the Repository:
Bash
git clone https://github.com/your-username/premav-sync.git
2. Install Dependencies:
Bash
pip install pandas numpy mysql-connector-python requests openpyxl
3. Configure Database: Update DB_CONFIG in premav.py with your MySQL host, user, and password.
4. Configure API: Update api_token in script.py with your REDCap project token.

Author
Yves Marie Ishimwe Kirunga Date: 2025-03-04

