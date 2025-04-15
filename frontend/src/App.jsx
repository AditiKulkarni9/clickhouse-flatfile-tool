// clickhouse-flatfile-tool/frontend/src/App.jsx
import { useState } from 'react';
import axios from 'axios';
import './App.css';

function App() {
    const [sourceType, setSourceType] = useState('');
    const [targetType, setTargetType] = useState('');
    const [clickHouseConfig, setClickHouseConfig] = useState({
        host: 'localhost',
        port: '9000',
        database: 'default',
        user: 'default',
        jwtToken: '',
    });
    const [flatFileConfig, setFlatFileConfig] = useState({ file: null, delimiter: ',' });
    const [tables, setTables] = useState([]);
    const [columns, setColumns] = useState([]);
    const [selectedTable, setSelectedTable] = useState('');
    const [selectedColumns, setSelectedColumns] = useState([]);
    const [status, setStatus] = useState('');
    const [recordCount, setRecordCount] = useState(null);

    const handleConnect = async () => {
        try {
            if (sourceType === 'clickhouse' || targetType === 'clickhouse') {
                const response = await fetch('http://localhost:8080/connect/clickhouse', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(clickHouseConfig),
                });
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}: ${await response.text()}`);
                }
                const data = await response.json();
                setStatus(data.message || 'Connected');
                if (sourceType === 'clickhouse') {
                    const tablesRes = await axios.get('http://localhost:8080/tables/clickhouse');
                    setTables(tablesRes.data.map(name => ({ name })));
                }
            } else {
                setStatus('No connection needed');
            }
        } catch (error) {
            console.error('Connect error:', error);
            setStatus(`Failed: ${error.message}`);
        }
    };

    const handleTableSelect = async (table) => {
        setSelectedTable(table);
        setStatus('Fetching columns...');
        try {
            if (sourceType === 'clickhouse') {
                const res = await axios.get(`http://localhost:8080/columns/clickhouse/${table}`);
                setColumns(res.data);
                setStatus('Columns loaded');
            }
        } catch (err) {
            setStatus(`Error: ${err.response?.data?.error || err.message}`);
        }
    };

    const handleFileUpload = async (e) => {
        const file = e.target.files[0];
        setFlatFileConfig({ ...flatFileConfig, file });
        const formData = new FormData();
        formData.append('file', file);
        formData.append('delimiter', flatFileConfig.delimiter);

        try {
            setStatus('Uploading file...');
            const res = await axios.post('http://localhost:8080/upload/flatfile', formData);
            const { filePath, delimiter } = res.data;
            const cols = await axios.get(`http://localhost:8080/columns/flatfile?filePath=${filePath}&delimiter=${delimiter}`);
            setColumns(cols.data);
            setFlatFileConfig({ ...flatFileConfig, file, filePath, delimiter });
            setStatus('File uploaded and columns loaded');
        } catch (err) {
            setStatus(`Error: ${err.response?.data?.error || err.message}`);
        }
    };

    const handleIngest = async () => {
        setStatus('Ingesting...');
        try {
            const payload = {
                source: sourceType,
                table: sourceType === 'flatfile' ? flatFileConfig.filePath : selectedTable,
                columns: selectedColumns,
                target: targetType,
                output: targetType === 'clickhouse' ? 'test_table' : 'output_uk_price_paid.csv',
            };
            const res = await axios.post('http://localhost:8080/ingest', payload);
            setRecordCount(res.data.recordCount);
            setStatus('Ingestion complete');
        } catch (err) {
            setStatus(`Error: ${err.response?.data?.error || err.message}`);
        }
    };

    return (
        <div className="container mx-auto p-4">
            <h1 className="text-2xl font-bold mb-4">ClickHouse & Flat File Ingestion Tool</h1>

            <div className="mb-4">
                <label className="block mb-2">Source:</label>
                <select
                    className="border p-2 rounded"
                    onChange={(e) => setSourceType(e.target.value)}
                    value={sourceType}
                >
                    <option value="">Select Source</option>
                    <option value="clickhouse">ClickHouse</option>
                    <option value="flatfile">Flat File</option>
                </select>

                <label className="block mb-2 mt-4">Target:</label>
                <select
                    className="border p-2 rounded"
                    onChange={(e) => setTargetType(e.target.value)}
                    value={targetType}
                >
                    <option value="">Select Target</option>
                    <option value="clickhouse">ClickHouse</option>
                    <option value="flatfile">Flat File</option>
                </select>
            </div>

            {(sourceType === 'clickhouse' || targetType === 'clickhouse') && (
                <div className="mb-4">
                    <h2 className="text-xl mb-2">ClickHouse Config</h2>
                    <input
                        type="text"
                        placeholder="Host"
                        className="border p-2 rounded mb-2 w-full"
                        value={clickHouseConfig.host}
                        onChange={(e) => setClickHouseConfig({ ...clickHouseConfig, host: e.target.value })}
                    />
                    <input
                        type="text"
                        placeholder="Port"
                        className="border p-2 rounded mb-2 w-full"
                        value={clickHouseConfig.port}
                        onChange={(e) => setClickHouseConfig({ ...clickHouseConfig, port: e.target.value })}
                    />
                    <input
                        type="text"
                        placeholder="Database"
                        className="border p-2 rounded mb-2 w-full"
                        value={clickHouseConfig.database}
                        onChange={(e) => setClickHouseConfig({ ...clickHouseConfig, database: e.target.value })}
                    />
                    <input
                        type="text"
                        placeholder="User"
                        className="border p-2 rounded mb-2 w-full"
                        value={clickHouseConfig.user}
                        onChange={(e) => setClickHouseConfig({ ...clickHouseConfig, user: e.target.value })}
                    />
                    <input
                        type="text"
                        placeholder="JWT Token"
                        className="border p-2 rounded mb-2 w-full"
                        value={clickHouseConfig.jwtToken}
                        onChange={(e) => setClickHouseConfig({ ...clickHouseConfig, jwtToken: e.target.value })}
                    />
                </div>
            )}

            {sourceType === 'flatfile' && (
                <div className="mb-4">
                    <h2 className="text-xl mb-2">Flat File Config</h2>
                    <input
                        type="file"
                        accept=".csv"
                        className="mb-2"
                        onChange={handleFileUpload}
                    />
                    <input
                        type="text"
                        placeholder="Delimiter"
                        className="border p-2 rounded mb-2 w-full"
                        value={flatFileConfig.delimiter}
                        onChange={(e) => setFlatFileConfig({ ...flatFileConfig, delimiter: e.target.value })}
                    />
                </div>
            )}

            <button
                className="bg-blue-500 text-white p-2 rounded mb-4"
                onClick={handleConnect}
            >
                Connect
            </button>

            {tables.length > 0 && sourceType === 'clickhouse' && (
                <div className="mb-4">
                    <h2 className="text-xl mb-2">Select Table</h2>
                    {tables.map((table) => (
                        <button
                            key={table.name}
                            className={`border p-2 rounded mr-2 ${selectedTable === table.name ? 'bg-blue-200' : ''}`}
                            onClick={() => handleTableSelect(table.name)}
                        >
                            {table.name}
                        </button>
                    ))}
                </div>
            )}

            {columns.length > 0 && (
                <div className="mb-4">
                    <h2 className="text-xl mb-2">Select Columns</h2>
                    {columns.map((col) => (
                        <div key={col.name}>
                            <input
                                type="checkbox"
                                checked={selectedColumns.includes(col.name)}
                                onChange={(e) => {
                                    if (e.target.checked) {
                                        setSelectedColumns([...selectedColumns, col.name]);
                                    } else {
                                        setSelectedColumns(selectedColumns.filter((c) => c !== col.name));
                                    }
                                }}
                            />
                            <span className="ml-2">{col.name} ({col.type})</span>
                        </div>
                    ))}
                </div>
            )}

            {selectedColumns.length > 0 && (
                <button
                    className="bg-green-500 text-white p-2 rounded mb-4"
                    onClick={handleIngest}
                >
                    Start Ingestion
                </button>
            )}

            <div>
                <p className="mb-2">Status: {status}</p>
                {recordCount !== null && (
                    <p>Records Processed: {recordCount}</p>
                )}
            </div>
        </div>
    );
}

export default App;