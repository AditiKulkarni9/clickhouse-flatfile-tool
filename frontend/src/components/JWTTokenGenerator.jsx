import React, { useState } from 'react';
import axios from 'axios';

const JWTTokenGenerator = ({ onTokenGenerated }) => {
  const [username, setUsername] = useState('');
  const [token, setToken] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const handleGenerateToken = async () => {
    if (!username) {
      setError('Please enter a username');
      return;
    }

    setLoading(true);
    setError('');
    
    try {
      const response = await axios.post('http://localhost:8080/auth/token', { username });
      const newToken = response.data.token;
      setToken(newToken);
      
      // Call the callback function with the new token
      if (onTokenGenerated) {
        onTokenGenerated(newToken);
      }
    } catch (err) {
      setError(`Error generating token: ${err.response?.data?.error || err.message}`);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="p-6 bg-white rounded-xl shadow-sm border border-gray-200">
      <h2 className="text-xl font-semibold text-gray-800 mb-4">JWT Token Generator</h2>
      
      <div className="space-y-4">
        <div>
          <label htmlFor="username" className="block text-sm font-medium text-gray-700">Username</label>
          <input
            id="username"
            type="text"
            placeholder="Enter username"
            className="mt-1 w-full p-2 border border-gray-300 rounded-md"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
          />
        </div>
        
        <button
          onClick={handleGenerateToken}
          disabled={loading}
          className={`w-full py-2 px-4 rounded-md text-white font-medium ${
            loading ? 'bg-gray-400' : 'bg-blue-600 hover:bg-blue-700'
          }`}
        >
          {loading ? 'Generating...' : 'Generate Token'}
        </button>
        
        {error && (
          <div className="text-red-600 text-sm">{error}</div>
        )}
        
        {token && (
          <div className="mt-4">
            <label className="block text-sm font-medium text-gray-700 mb-1">Generated Token</label>
            <div className="p-3 bg-gray-100 rounded-md text-sm font-mono break-all">
              {token}
            </div>
            <button
              onClick={() => navigator.clipboard.writeText(token)}
              className="mt-2 text-sm text-blue-600 hover:text-blue-800"
            >
              Copy to Clipboard
            </button>
          </div>
        )}
      </div>
    </div>
  );
};

export default JWTTokenGenerator; 