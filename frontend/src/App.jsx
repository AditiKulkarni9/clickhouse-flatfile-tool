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
        database: 'uk',
        user: 'default',
        jwtToken: '',
    });
    const [flatFileConfig, setFlatFileConfig] = useState({ file: null, delimiter: ',', filePath: '' });
    const [tables, setTables] = useState([]);
    const [columns, setColumns] = useState([]);
    const [selectedTable, setSelectedTable] = useState('');
    const [selectedColumns, setSelectedColumns] = useState([]);
    const [status, setStatus] = useState({ message: '', type: '' });
    const [recordCount, setRecordCount] = useState(null);
    const [previewData, setPreviewData] = useState(null);

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
            setStatus({ message: `Connection failed: ${err.response?.data?.error || err.message}`, type: 'error' });
        }
    };

    const handleTableSelect = async (table) => {
        setSelectedTable(table);
        setColumns([]);
        setSelectedColumns([]);
        setPreviewData(null);
    };

    const handleLoadColumns = async () => {
        setStatus({ message: 'Fetching columns...', type: 'loading' });
        try {
            let endpoint;
            if (sourceType === 'clickhouse') {
                endpoint = `http://localhost:8080/columns/clickhouse/${selectedTable}`;
            } else {
                endpoint = `http://localhost:8080/columns/flatfile?filePath=${encodeURIComponent(flatFileConfig.filePath)}&delimiter=${flatFileConfig.delimiter}`;
            }
            const res = await axios.get(endpoint);
            const columnList = res.data.map(col => col.name);
            setColumns(columnList);
            setStatus({ message: 'Columns loaded', type: 'success' });
        } catch (err) {
            setStatus({ message: `Error: ${err.response?.data?.error || err.message}`, type: 'error' });
        }
    };

    const handleFileUpload = async (e) => {
        const file = e.target.files[0];
        if (!file) return;
        setFlatFileConfig({ ...flatFileConfig, file, filePath: '' });
        const formData = new FormData();
        formData.append('file', file);
        formData.append('delimiter', flatFileConfig.delimiter);

        setStatus({ message: 'Uploading file...', type: 'loading' });
        try {
            const res = await axios.post('http://localhost:8080/upload/flatfile', formData);
            const { filePath, delimiter } = res.data;
            setFlatFileConfig({ ...flatFileConfig, file, filePath, delimiter });
            setStatus({ message: 'File uploaded successfully', type: 'success' });
            setSelectedTable(filePath);
            setColumns([]);
            setSelectedColumns([]);
            setPreviewData(null);
        } catch (err) {
            setStatus({ message: `File upload failed: ${err.response?.data?.error || err.message}`, type: 'error' });
        }
    };

    const handlePreview = async () => {
        setStatus({ message: 'Loading preview...', type: 'loading' });
        try {
            const payload = {
                source: sourceType,
                table: sourceType === 'flatfile' ? flatFileConfig.filePath : selectedTable,
                columns: selectedColumns,
            };
            const res = await axios.post('http://localhost:8080/preview', payload);
            setPreviewData(res.data);
            setStatus({ message: 'Preview loaded', type: 'success' });
        } catch (err) {
            setStatus({ message: `Preview failed: ${err.response?.data?.error || err.message}`, type: 'error' });
            setPreviewData(null);
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
                output: targetType === 'clickhouse' ? 'uk_price_paid_import' : 'output_uk_price_paid.csv',
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

    const isLoadColumnsDisabled = !selectedTable || 
        (sourceType === 'flatfile' && !flatFileConfig.filePath);

    const isPreviewDisabled = selectedColumns.length === 0;

    const isIngestDisabled = !selectedTable || selectedColumns.length === 0 || 
        (sourceType === 'flatfile' && !flatFileConfig.filePath);

    return (
        <div className="container mx-auto p-6 max-w-4xl bg-gray-100 min-h-screen">
            <h1 className="text-3xl font-bold text-gray-900 mb-8 text-center">Data Ingestion Tool</h1>

            {/* Source and Target Selection */}
            <section className="mb-8 p-6 bg-white rounded-lg shadow">
                <h2 className="text-xl font-semibold text-gray-800 mb-4">Step 1: Select Source and Target</h2>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    <div>
                        <label htmlFor="source" className="block text-sm font-medium text-gray-700 mb-2">Source</label>
                        <select
                            id="source"
                            className="w-full p-3 border rounded-lg focus:ring-2 focus:ring-blue-600"
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
                        <label htmlFor="target" className="block text-sm font-medium text-gray-700 mb-2">Target</label>
                        <select
                            id="target"
                            className="w-full p-3 border rounded-lg focus:ring-2 focus:ring-blue-600"
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

            {/* ClickHouse Configuration */}
            {(sourceType === 'clickhouse' || targetType === 'clickhouse') && (
                <section className="mb-8 p-6 bg-white rounded-lg shadow">
                    <h2 className="text-xl font-semibold text-gray-800 mb-4">Step 2: Configure ClickHouse</h2>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                        <div>
                            <label htmlFor="host" className="block text-sm font-medium text-gray-700 mb-2">Host</label>
                            <input
                                id="host"
                                type="text"
                                placeholder="e.g., localhost"
                                className="w-full p-3 border rounded-lg focus:ring-2 focus:ring-blue-600"
                                value={clickHouseConfig.host}
                                onChange={(e) => setClickHouseConfig({ ...clickHouseConfig, host: e.target.value })}
                            />
                        </div>
                        <div>
                            <label htmlFor="port" className="block text-sm font-medium text-gray-700 mb-2">Port</label>
                            <input
                                id="port"
                                type="text"
                                placeholder="e.g., 9000"
                                className="w-full p-3 border rounded-lg focus:ring-2 focus:ring-blue-600"
                                value={clickHouseConfig.port}
                                onChange={(e) => setClickHouseConfig({ ...clickHouseConfig, port: e.target.value })}
                            />
                        </div>
                        <div>
                            <label htmlFor="database" className="block text-sm font-medium text-gray-700 mb-2">Database</label>
                            <input
                                id="database"
                                type="text"
                                placeholder="e.g., uk"
                                className="w-full p-3 border rounded-lg focus:ring-2 focus:ring-blue-600"
                                value={clickHouseConfig.database}
                                onChange={(e) => setClickHouseConfig({ ...clickHouseConfig, database: e.target.value })}
                            />
                        </div>
                        <div>
                            <label htmlFor="user" className="block text-sm font-medium text-gray-700 mb-2">User</label>
                            <input
                                id="user"
                                type="text"
                                placeholder="e.g., default"
                                className="w-full p-3 border rounded-lg focus:ring-2 focus:ring-blue-600"
                                value={clickHouseConfig.user}
                                onChange={(e) => setClickHouseConfig({ ...clickHouseConfig, user: e.target.value })}
                            />
                        </div>
                        <div>
                            <label htmlFor="jwtToken" className="block text-sm font-medium text-gray-700 mb-2">JWT Token</label>
                            <input
                                id="jwtToken"
                                type="text"
                                placeholder="Optional"
                                className="w-full p-3 border rounded-lg focus:ring-2 focus:ring-blue-600"
                                value={clickHouseConfig.jwtToken}
                                onChange={(e) => setClickHouseConfig({ ...clickHouseConfig, jwtToken: e.target.value })}
                            />
                        </div>
                    </div>
                </section>
            )}

            {/* Flat File Configuration */}
            {sourceType === 'flatfile' && (
                <section className="mb-8 p-6 bg-white rounded-lg shadow">
                    <h2 className="text-xl font-semibold text-gray-800 mb-4">Step 2: Upload CSV File</h2>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                        <div>
                            <label htmlFor="file" className="block text-sm font-medium text-gray-700 mb-2">CSV File</label>
                            <input
                                id="file"
                                type="file"
                                accept=".csv"
                                className="w-full p-3 border rounded-lg"
                                onChange={handleFileUpload}
                            />
                        </div>
                        <div>
                            <label htmlFor="delimiter" className="block text-sm font-medium text-gray-700 mb-2">Delimiter</label>
                            <input
                                id="delimiter"
                                type="text"
                                placeholder="e.g., ,"
                                className="w-full p-3 border rounded-lg focus:ring-2 focus:ring-blue-600"
                                value={flatFileConfig.delimiter}
                                onChange={(e) => setFlatFileConfig({ ...flatFileConfig, delimiter: e.target.value })}
                            />
                        </div>
                    </div>
                </section>
            )}

            {/* Connect Button */}
            <section className="mb-8 flex justify-center">
                <button
                    className={`px-6 py-3 rounded-lg text-white font-medium ${
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
                <section className="mb-8 p-6 bg-white rounded-lg shadow">
                    <h2 className="text-xl font-semibold text-gray-800 mb-4">Step 3: Select Table</h2>
                    <div className="flex flex-wrap gap-3">
                        {tables.map((table) => (
                            <button
                                key={table.name}
                                className={`px-4 py-2 rounded-lg border ${
                                    selectedTable === table.name
                                        ? 'bg-blue-100 border-blue-600'
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

            {/* Load Columns Button */}
            {selectedTable && (
                <section className="mb-8 p-6 bg-white rounded-lg shadow">
                    <h2 className="text-xl font-semibold text-gray-800 mb-4">
                        {sourceType === 'flatfile' ? 'Step 3: Load CSV Columns' : 'Step 4: Load Table Columns'}
                    </h2>
                    <button
                        className={`px-6 py-3 rounded-lg text-white font-medium ${
                            isLoadColumnsDisabled ? 'bg-gray-400 cursor-not-allowed' : 'bg-purple-600 hover:bg-purple-700'
                        }`}
                        onClick={handleLoadColumns}
                        disabled={isLoadColumnsDisabled}
                        aria-disabled={isLoadColumnsDisabled}
                    >
                        Load Columns
                    </button>
                </section>
            )}

            {/* Column Selection */}
            {columns.length > 0 && (
                <section className="mb-8 p-6 bg-white rounded-lg shadow">
                    <h2 className="text-xl font-semibold text-gray-800 mb-4">
                        {sourceType === 'flatfile' ? 'Step 4: Select Columns' : 'Step 5: Select Columns'}
                    </h2>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                        {columns.map((col) => (
                            <label key={col} className="flex items-center space-x-2">
                                <input
                                    type="checkbox"
                                    checked={selectedColumns.includes(col)}
                                    onChange={(e) => {
                                        if (e.target.checked) {
                                            setSelectedColumns([...selectedColumns, col]);
                                        } else {
                                            setSelectedColumns(selectedColumns.filter((c) => c !== col));
                                        }
                                    }}
                                    className="h-4 w-4 text-blue-600"
                                />
                                <span className="text-gray-700">{col}</span>
                            </label>
                        ))}
                    </div>
                </section>
            )}

            {/* Preview Button and Table */}
            {columns.length > 0 && (
                <section className="mb-8 p-6 bg-white rounded-lg shadow">
                    <h2 className="text-xl font-semibold text-gray-800 mb-4">
                        {sourceType === 'flatfile' ? 'Step 5: Preview Data' : 'Step 6: Preview Data'}
                    </h2>
                    <button
                        className={`px-6 py-3 rounded-lg text-white font-medium ${
                            isPreviewDisabled ? 'bg-gray-400 cursor-not-allowed' : 'bg-indigo-600 hover:bg-indigo-700'
                        }`}
                        onClick={handlePreview}
                        disabled={isPreviewDisabled}
                        aria-disabled={isPreviewDisabled}
                    >
                        Preview
                    </button>
                    {previewData && (
                        <div className="mt-6 overflow-x-auto">
                            <table className="min-w-full border-collapse border border-gray-300">
                                <thead>
                                    <tr className="bg-gray-100">
                                        {previewData.headers.map((header, index) => (
                                            <th key={index} className="border border-gray-300 px-4 py-2 text-left text-gray-800">
                                                {header}
                                            </th>
                                        ))}
                                    </tr>
                                </thead>
                                <tbody>
                                    {previewData.rows.map((row, rowIndex) => (
                                        <tr key={rowIndex} className="hover:bg-gray-50">
                                            {row.map((cell, cellIndex) => (
                                                <td key={cellIndex} className="border border-gray-300 px-4 py-2 text-gray-700">
                                                    {cell}
                                                </td>
                                            ))}
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    )}
                </section>
            )}

            {/* Ingest Button */}
            <section className="mb-8 flex justify-center">
                <button
                    className={`px-6 py-3 rounded-lg text-white font-medium ${
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
            <section className="p-6 bg-white rounded-lg shadow">
                <h2 className="text-xl font-semibold text-gray-800 mb-4">Status</h2>
                <div className="flex items-center space-x-3">
                    {status.type === 'loading' && <FaSpinner className="animate-spin text-blue-600" />}
                    {status.type === 'success' && <FaCheckCircle className="text-green-600" />}
                    {status.type === 'error' && <FaExclamationTriangle className="text-red-600" />}
                    <p className={`text-gray-700 ${status.type === 'error' ? 'text-red-600' : ''}`}>
                        {status.message || 'Ready'}
                    </p>
                </div>
                {recordCount !== null && (
                    <p className="mt-3 text-gray-700">Records Processed: {recordCount}</p>
                )}
            </section>
        </div>
    );
}

export default App;