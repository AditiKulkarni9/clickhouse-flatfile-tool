import { useState } from 'react';
import axios from 'axios';
import { FaCheckCircle, FaExclamationTriangle, FaSpinner, FaDatabase, FaFileAlt, FaArrowRight } from 'react-icons/fa';
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
        <div className="min-h-screen bg-gradient-to-br from-gray-50 to-gray-100">
            <div className="container mx-auto p-6 max-w-5xl">
                <header className="text-center mb-12">
                    <h1 className="text-4xl font-bold text-gray-900 mb-2">Data Ingestion Tool</h1>
                    <p className="text-gray-600">Transfer data between ClickHouse and flat files seamlessly</p>
                </header>

                {/* Status Message */}
                {status.message && (
                    <div className={`mb-6 p-4 rounded-lg ${
                        status.type === 'success' ? 'bg-green-50 text-green-800' :
                        status.type === 'error' ? 'bg-red-50 text-red-800' :
                        'bg-blue-50 text-blue-800'
                    }`}>
                        <div className="flex items-center">
                            {status.type === 'success' && <FaCheckCircle className="mr-2" />}
                            {status.type === 'error' && <FaExclamationTriangle className="mr-2" />}
                            {status.type === 'loading' && <FaSpinner className="mr-2 animate-spin" />}
                            <span>{status.message}</span>
                        </div>
                    </div>
                )}

                {/* Source and Target Selection */}
                <section className="mb-8 p-8 bg-white rounded-xl shadow-sm border border-gray-200">
                    <h2 className="text-xl font-semibold text-gray-800 mb-6">Step 1: Select Source and Target</h2>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                        <div className="space-y-4">
                            <label htmlFor="source" className="block text-sm font-medium text-gray-700">Source</label>
                            <div className="relative">
                                <select
                                    id="source"
                                    className="w-full p-4 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 appearance-none bg-white"
                                    onChange={(e) => setSourceType(e.target.value)}
                                    value={sourceType}
                                >
                                    <option value="">Choose a source</option>
                                    <option value="clickhouse">ClickHouse Database</option>
                                    <option value="flatfile">Flat File (CSV)</option>
                                </select>
                                <div className="absolute inset-y-0 right-0 flex items-center px-3 pointer-events-none">
                                    <FaDatabase className="text-gray-400" />
                                </div>
                            </div>
                        </div>
                        <div className="space-y-4">
                            <label htmlFor="target" className="block text-sm font-medium text-gray-700">Target</label>
                            <div className="relative">
                                <select
                                    id="target"
                                    className="w-full p-4 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 appearance-none bg-white"
                                    onChange={(e) => setTargetType(e.target.value)}
                                    value={targetType}
                                >
                                    <option value="">Choose a target</option>
                                    <option value="clickhouse">ClickHouse Database</option>
                                    <option value="flatfile">Flat File (CSV)</option>
                                </select>
                                <div className="absolute inset-y-0 right-0 flex items-center px-3 pointer-events-none">
                                    <FaFileAlt className="text-gray-400" />
                                </div>
                            </div>
                        </div>
                    </div>
                </section>

                {/* ClickHouse Configuration */}
                {(sourceType === 'clickhouse' || targetType === 'clickhouse') && (
                    <section className="mb-8 p-8 bg-white rounded-xl shadow-sm border border-gray-200">
                        <h2 className="text-xl font-semibold text-gray-800 mb-6">Step 2: Configure ClickHouse</h2>
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                            <div className="space-y-2">
                                <label htmlFor="host" className="block text-sm font-medium text-gray-700">Host</label>
                                <input
                                    id="host"
                                    type="text"
                                    placeholder="e.g., localhost"
                                    className="w-full p-4 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                                    value={clickHouseConfig.host}
                                    onChange={(e) => setClickHouseConfig({ ...clickHouseConfig, host: e.target.value })}
                                />
                            </div>
                            <div className="space-y-2">
                                <label htmlFor="port" className="block text-sm font-medium text-gray-700">Port</label>
                                <input
                                    id="port"
                                    type="text"
                                    placeholder="e.g., 9000"
                                    className="w-full p-4 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                                    value={clickHouseConfig.port}
                                    onChange={(e) => setClickHouseConfig({ ...clickHouseConfig, port: e.target.value })}
                                />
                            </div>
                            <div className="space-y-2">
                                <label htmlFor="database" className="block text-sm font-medium text-gray-700">Database</label>
                                <input
                                    id="database"
                                    type="text"
                                    placeholder="e.g., uk"
                                    className="w-full p-4 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                                    value={clickHouseConfig.database}
                                    onChange={(e) => setClickHouseConfig({ ...clickHouseConfig, database: e.target.value })}
                                />
                            </div>
                            <div className="space-y-2">
                                <label htmlFor="user" className="block text-sm font-medium text-gray-700">User</label>
                                <input
                                    id="user"
                                    type="text"
                                    placeholder="e.g., default"
                                    className="w-full p-4 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                                    value={clickHouseConfig.user}
                                    onChange={(e) => setClickHouseConfig({ ...clickHouseConfig, user: e.target.value })}
                                />
                            </div>
                            <div className="space-y-2 md:col-span-2">
                                <label htmlFor="jwtToken" className="block text-sm font-medium text-gray-700">JWT Token (Optional)</label>
                                <input
                                    id="jwtToken"
                                    type="password"
                                    placeholder="Enter JWT token if required"
                                    className="w-full p-4 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                                    value={clickHouseConfig.jwtToken}
                                    onChange={(e) => setClickHouseConfig({ ...clickHouseConfig, jwtToken: e.target.value })}
                                />
                            </div>
                        </div>
                        <div className="mt-6">
                            <button
                                onClick={handleConnect}
                                disabled={isConnectDisabled}
                                className={`w-full py-3 px-4 rounded-lg text-white font-medium ${
                                    isConnectDisabled
                                        ? 'bg-gray-400 cursor-not-allowed'
                                        : 'bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2'
                                }`}
                            >
                                Connect to ClickHouse
                            </button>
                        </div>
                    </section>
                )}

                {/* File Upload Section */}
                {sourceType === 'flatfile' && (
                    <section className="mb-8 p-8 bg-white rounded-xl shadow-sm border border-gray-200">
                        <h2 className="text-xl font-semibold text-gray-800 mb-6">Step 2: Upload File</h2>
                        <div className="space-y-4">
                            <div className="flex items-center justify-center w-full">
                                <label className="flex flex-col items-center justify-center w-full h-64 border-2 border-gray-300 border-dashed rounded-lg cursor-pointer bg-gray-50 hover:bg-gray-100">
                                    <div className="flex flex-col items-center justify-center pt-5 pb-6">
                                        <FaFileAlt className="w-10 h-10 mb-3 text-gray-400" />
                                        <p className="mb-2 text-sm text-gray-500">
                                            <span className="font-semibold">Click to upload</span> or drag and drop
                                        </p>
                                        <p className="text-xs text-gray-500">CSV files only</p>
                                    </div>
                                    <input
                                        type="file"
                                        className="hidden"
                                        accept=".csv"
                                        onChange={handleFileUpload}
                                    />
                                </label>
                            </div>
                            {flatFileConfig.file && (
                                <div className="text-sm text-gray-600">
                                    Selected file: {flatFileConfig.file.name}
                                </div>
                            )}
                        </div>
                    </section>
                )}

                {/* Table Selection and Column Selection */}
                <section className="mb-8 p-8 bg-white rounded-xl shadow-sm border border-gray-200">
                    <h2 className="text-xl font-semibold text-gray-800 mb-6">Step 3: Select Data</h2>
                    <div className="space-y-6">
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                            <div className="space-y-4">
                                <label className="block text-sm font-medium text-gray-700">Available Tables</label>
                                <div className="border border-gray-300 rounded-lg divide-y">
                                    {tables.map((table) => (
                                        <button
                                            key={table.name}
                                            onClick={() => handleTableSelect(table.name)}
                                            className={`w-full p-4 text-left hover:bg-gray-50 ${
                                                selectedTable === table.name ? 'bg-blue-50 text-blue-700' : ''
                                            }`}
                                        >
                                            {table.name}
                                        </button>
                                    ))}
                                </div>
                            </div>
                            <div className="space-y-4">
                                <label className="block text-sm font-medium text-gray-700">Available Columns</label>
                                <div className="border border-gray-300 rounded-lg divide-y max-h-64 overflow-y-auto">
                                    {columns.map((column) => (
                                        <label
                                            key={column}
                                            className="flex items-center p-4 hover:bg-gray-50 cursor-pointer"
                                        >
                                            <input
                                                type="checkbox"
                                                checked={selectedColumns.includes(column)}
                                                onChange={(e) => {
                                                    if (e.target.checked) {
                                                        setSelectedColumns([...selectedColumns, column]);
                                                    } else {
                                                        setSelectedColumns(selectedColumns.filter(c => c !== column));
                                                    }
                                                }}
                                                className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                                            />
                                            <span className="ml-3 text-sm text-gray-700">{column}</span>
                                        </label>
                                    ))}
                                </div>
                            </div>
                        </div>
                        <div className="flex justify-end space-x-4">
                            <button
                                onClick={handleLoadColumns}
                                disabled={isLoadColumnsDisabled}
                                className={`py-2 px-4 rounded-lg text-white font-medium ${
                                    isLoadColumnsDisabled
                                        ? 'bg-gray-400 cursor-not-allowed'
                                        : 'bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2'
                                }`}
                            >
                                Load Columns
                            </button>
                            <button
                                onClick={handlePreview}
                                disabled={isPreviewDisabled}
                                className={`py-2 px-4 rounded-lg text-white font-medium ${
                                    isPreviewDisabled
                                        ? 'bg-gray-400 cursor-not-allowed'
                                        : 'bg-green-600 hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-green-500 focus:ring-offset-2'
                                }`}
                            >
                                Preview Data
                            </button>
                        </div>
                    </div>
                </section>

                {/* Preview Section */}
                {previewData && (
                    <section className="mb-8 p-8 bg-white rounded-xl shadow-sm border border-gray-200">
                        <h2 className="text-xl font-semibold text-gray-800 mb-6">Data Preview</h2>
                        <div className="overflow-x-auto">
                            <table className="min-w-full divide-y divide-gray-200">
                                <thead className="bg-gray-50">
                                    <tr>
                                        {previewData.headers.map((header) => (
                                            <th
                                                key={header}
                                                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                                            >
                                                {header}
                                            </th>
                                        ))}
                                    </tr>
                                </thead>
                                <tbody className="bg-white divide-y divide-gray-200">
                                    {previewData.rows.map((row, rowIndex) => (
                                        <tr key={rowIndex} className="hover:bg-gray-50">
                                            {row.map((cell, cellIndex) => (
                                                <td
                                                    key={cellIndex}
                                                    className="px-6 py-4 whitespace-nowrap text-sm text-gray-500"
                                                >
                                                    {cell}
                                                </td>
                                            ))}
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    </section>
                )}

                {/* Ingest Button */}
                <div className="flex justify-center">
                    <button
                        onClick={handleIngest}
                        disabled={isIngestDisabled}
                        className={`py-4 px-8 rounded-lg text-white font-medium text-lg ${
                            isIngestDisabled
                                ? 'bg-gray-400 cursor-not-allowed'
                                : 'bg-green-600 hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-green-500 focus:ring-offset-2'
                        }`}
                    >
                        {isIngestDisabled ? 'Select Data to Ingest' : 'Start Ingestion'}
                    </button>
                </div>

                {/* Record Count */}
                {recordCount !== null && (
                    <div className="mt-6 text-center text-gray-600">
                        Successfully processed {recordCount} records
                    </div>
                )}
            </div>
        </div>
    );
}

export default App;