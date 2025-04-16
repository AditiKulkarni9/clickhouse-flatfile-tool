// clickhouse-flatfile-tool/frontend/src/App.jsx
import { useState } from 'react';
import axios from 'axios';
import { FaCheckCircle, FaExclamationTriangle, FaSpinner } from 'react-icons/fa';
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
    const [flatFileConfig, setFlatFileConfig] = useState({ file: null, delimiter: ',', filePath: '' });
    const [tables, setTables] = useState([]);
    const [columns, setColumns] = useState([]);
    const [selectedTable, setSelectedTable] = useState('');
    const [selectedColumns, setSelectedColumns] = useState([]);
    const [status, setStatus] = useState({ message: '', type: '' }); // success, error, loading
    const [recordCount, setRecordCount] = useState(null);

    const handleConnect = async () => {
        setStatus({ message: 'Connecting...', type: 'loading' });
        try {
            if (sourceType === 'clickhouse' || targetType === 'clickhouse') {
                const response = await axios.post('http://localhost:8080/connect/clickhouse', clickHouseConfig);
                setStatus({ message: response.data.message || 'Connected to ClickHouse', type: 'success' });
                if (sourceType === 'clickhouse') {
                    const tablesRes = await axios.get('http://localhost:8080/tables/clickhouse');
                    setTables(tablesRes.data.map(name => ({ name })));
                }
            } else {
                setStatus({ message: 'No connection needed for flat file only', type: 'success' });
            }
        } catch (err) {
            setStatus({ message: `Connection failed2: ${err.response?.data?.error || err.message}`, type: 'error' });
        }
    };

    const handleTableSelect = async (table) => {
        setSelectedTable(table);
        setColumns([]);
        setSelectedColumns([]);
        setStatus({ message: 'Fetching columns...', type: 'loading' });
        try {
            const endpoint = sourceType === 'clickhouse' 
                ? `http://localhost:8080/columns/clickhouse/${table}`
                : `http://localhost:8080/columns/flatfile?filePath=${flatFileConfig.filePath}&delimiter=${flatFileConfig.delimiter}`;
            const res = await axios.get(endpoint);
            setColumns(res.data);
            setStatus({ message: 'Columns loaded', type: 'success' });
        } catch (err) {
            setStatus({ message: `Error: ${err.response?.data?.error || err.message}`, type: 'error' });
        }
    };

    const handleFileUpload = async (e) => {
        const file = e.target.files[0];
        if (!file) return;
        setFlatFileConfig({ ...flatFileConfig, file });
        const formData = new FormData();
        formData.append('file', file);
        formData.append('delimiter', flatFileConfig.delimiter);

        setStatus({ message: 'Uploading file...', type: 'loading' });
        try {
            const res = await axios.post('http://localhost:8080/upload/flatfile', formData);
            const { filePath, delimiter } = res.data;
            setFlatFileConfig({ ...flatFileConfig, file, filePath, delimiter });
            await handleTableSelect(filePath);
        } catch (err) {
            setStatus({ message: `Error: ${err.response?.data?.error || err.message}`, type: 'error' });
        }
    };

    const handleIngest = async () => {
        setStatus({ message: 'Ingesting data...', type: 'loading' });
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
            setStatus({ message: 'Ingestion complete', type: 'success' });
        } catch (err) {
            setStatus({ message: `Error: ${err.response?.data?.error || err.message}`, type: 'error' });
        }
    };

    const isConnectDisabled = !sourceType || !targetType || 
        ((sourceType === 'clickhouse' || targetType === 'clickhouse') && 
         (!clickHouseConfig.host || !clickHouseConfig.port || !clickHouseConfig.database));

    const isIngestDisabled = !selectedTable || selectedColumns.length === 0 || 
        (sourceType === 'flatfile' && !flatFileConfig.filePath);

    return (
        <div className="container mx-auto p-6 max-w-4xl bg-gray-50 min-h-screen">
            <h1 className="text-3xl font-bold text-gray-800 mb-6">Data Ingestion Tool</h1>

            {/* Source and Target Selection */}
            <section className="mb-8 p-6 bg-white rounded-lg shadow-md">
                <h2 className="text-xl font-semibold text-gray-700 mb-4">Step 1: Select Source and Target</h2>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                        <label htmlFor="source" className="block text-sm font-medium text-gray-600 mb-1">Source</label>
                        <select
                            id="source"
                            className="w-full p-2 border rounded-md focus:ring-2 focus:ring-blue-500"
                            onChange={(e) => setSourceType(e.target.value)}
                            value={sourceType}
                            aria-describedby="source-help"
                        >
                            <option value="">Choose a source</option>
                            <option value="clickhouse">ClickHouse</option>
                            <option value="flatfile">Flat File (CSV)</option>
                        </select>
                        <p id="source-help" className="text-xs text-gray-500 mt-1">Select where your data is coming from.</p>
                    </div>
                    <div>
                        <label htmlFor="target" className="block text-sm font-medium text-gray-600 mb-1">Target</label>
                        <select
                            id="target"
                            className="w-full p-2 border rounded-md focus:ring-2 focus:ring-blue-500"
                            onChange={(e) => setTargetType(e.target.value)}
                            value={targetType}
                            aria-describedby="target-help"
                        >
                            <option value="">Choose a target</option>
                            <option value="clickhouse">ClickHouse</option>
                            <option value="flatfile">Flat File (CSV)</option>
                        </select>
                        <p id="target-help" className="text-xs text-gray-500 mt-1">Select where your data will be saved.</p>
                    </div>
                </div>
            </section>

            {/* Configuration */}
            {(sourceType === 'clickhouse' || targetType === 'clickhouse') && (
                <section className="mb-8 p-6 bg-white rounded-lg shadow-md">
                    <h2 className="text-xl font-semibold text-gray-700 mb-4">Step 2: Configure ClickHouse</h2>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div>
                            <label htmlFor="host" className="block text-sm font-medium text-gray-600 mb-1">Host</label>
                            <input
                                id="host"
                                type="text"
                                placeholder="e.g., localhost"
                                className="w-full p-2 border rounded-md focus:ring-2 focus:ring-blue-500"
                                value={clickHouseConfig.host}
                                onChange={(e) => setClickHouseConfig({ ...clickHouseConfig, host: e.target.value })}
                            />
                        </div>
                        <div>
                            <label htmlFor="port" className="block text-sm font-medium text-gray-600 mb-1">Port</label>
                            <input
                                id="port"
                                type="text"
                                placeholder="e.g., 9000"
                                className="w-full p-2 border rounded-md focus:ring-2 focus:ring-blue-500"
                                value={clickHouseConfig.port}
                                onChange={(e) => setClickHouseConfig({ ...clickHouseConfig, port: e.target.value })}
                            />
                        </div>
                        <div>
                            <label htmlFor="database" className="block text-sm font-medium text-gray-600 mb-1">Database</label>
                            <input
                                id="database"
                                type="text"
                                placeholder="e.g., default"
                                className="w-full p-2 border rounded-md focus:ring-2 focus:ring-blue-500"
                                value={clickHouseConfig.database}
                                onChange={(e) => setClickHouseConfig({ ...clickHouseConfig, database: e.target.value })}
                            />
                        </div>
                        <div>
                            <label htmlFor="user" className="block text-sm font-medium text-gray-600 mb-1">User</label>
                            <input
                                id="user"
                                type="text"
                                placeholder="e.g., default"
                                className="w-full p-2 border rounded-md focus:ring-2 focus:ring-blue-500"
                                value={clickHouseConfig.user}
                                onChange={(e) => setClickHouseConfig({ ...clickHouseConfig, user: e.target.value })}
                            />
                        </div>
                        <div>
                            <label htmlFor="jwtToken" className="block text-sm font-medium text-gray-600 mb-1">JWT Token</label>
                            <input
                                id="jwtToken"
                                type="text"
                                placeholder="Optional"
                                className="w-full p-2 border rounded-md focus:ring-2 focus:ring-blue-500"
                                value={clickHouseConfig.jwtToken}
                                onChange={(e) => setClickHouseConfig({ ...clickHouseConfig, jwtToken: e.target.value })}
                            />
                        </div>
                    </div>
                </section>
            )}

            {sourceType === 'flatfile' && (
                <section className="mb-8 p-6 bg-white rounded-lg shadow-md">
                    <h2 className="text-xl font-semibold text-gray-700 mb-4">Step 2: Upload CSV File</h2>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div>
                            <label htmlFor="file" className="block text-sm font-medium text-gray-600 mb-1">CSV File</label>
                            <input
                                id="file"
                                type="file"
                                accept=".csv"
                                className="w-full p-2 border rounded-md"
                                onChange={handleFileUpload}
                            />
                        </div>
                        <div>
                            <label htmlFor="delimiter" className="block text-sm font-medium text-gray-600 mb-1">Delimiter</label>
                            <input
                                id="delimiter"
                                type="text"
                                placeholder="e.g., ,"
                                className="w-full p-2 border rounded-md focus:ring-2 focus:ring-blue-500"
                                value={flatFileConfig.delimiter}
                                onChange={(e) => setFlatFileConfig({ ...flatFileConfig, delimiter: e.target.value })}
                            />
                        </div>
                    </div>
                </section>
            )}

            {/* Connect Button */}
            <section className="mb-8">
                <button
                    className={`w-full md:w-auto px-6 py-3 rounded-md text-white font-medium transition-colors ${
                        isConnectDisabled ? 'bg-gray-400 cursor-not-allowed' : 'bg-blue-600 hover:bg-blue-700'
                    }`}
                    onClick={handleConnect}
                    disabled={isConnectDisabled}
                    aria-disabled={isConnectDisabled}
                >
                    Connect
                </button>
            </section>

            {/* Table Selection */}
            {tables.length > 0 && sourceType === 'clickhouse' && (
                <section className="mb-8 p-6 bg-white rounded-lg shadow-md">
                    <h2 className="text-xl font-semibold text-gray-700 mb-4">Step 3: Select Table</h2>
                    <div className="flex flex-wrap gap-2">
                        {tables.map((table) => (
                            <button
                                key={table.name}
                                className={`px-4 py-2 rounded-md border transition-colors ${
                                    selectedTable === table.name
                                        ? 'bg-blue-100 border-blue-500'
                                        : 'border-gray-300 hover:bg-gray-100'
                                }`}
                                onClick={() => handleTableSelect(table.name)}
                            >
                                {table.name}
                            </button>
                        ))}
                    </div>
                </section>
            )}

            {/* Column Selection */}
            {columns.length > 0 && (
                <section className="mb-8 p-6 bg-white rounded-lg shadow-md">
                    <h2 className="text-xl font-semibold text-gray-700 mb-4">Step 4: Select Columns</h2>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                        {columns.map((col) => (
                            <label key={col.name} className="flex items-center space-x-2">
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
                                    className="h-4 w-4 text-blue-600"
                                />
                                <span className="text-gray-700">{col.name} ({col.type})</span>
                            </label>
                        ))}
                    </div>
                </section>
            )}

            {/* Ingest Button */}
            <section className="mb-8">
                <button
                    className={`w-full md:w-auto px-6 py-3 rounded-md text-white font-medium transition-colors ${
                        isIngestDisabled ? 'bg-gray-400 cursor-not-allowed' : 'bg-green-600 hover:bg-green-700'
                    }`}
                    onClick={handleIngest}
                    disabled={isIngestDisabled}
                    aria-disabled={isIngestDisabled}
                >
                    Start Ingestion
                </button>
            </section>

            {/* Status and Results */}
            <section className="p-6 bg-white rounded-lg shadow-md">
                <h2 className="text-xl font-semibold text-gray-700 mb-4">Status</h2>
                <div className="flex items-center space-x-2">
                    {status.type === 'loading' && <FaSpinner className="animate-spin text-blue-600" />}
                    {status.type === 'success' && <FaCheckCircle className="text-green-600" />}
                    {status.type === 'error' && <FaExclamationTriangle className="text-red-600" />}
                    <p className={`text-gray-700 ${status.type === 'error' ? 'text-red-600' : ''}`}>
                        {status.message || 'Ready'}
                    </p>
                </div>
                {recordCount !== null && (
                    <p className="mt-2 text-gray-700">Records Processed: {recordCount}</p>
                )}
            </section>
        </div>
    );
}

export default App;