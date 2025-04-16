// handlers/flatfile.go
package handlers

import (
    "net/http"
    "github.com/gin-gonic/gin"
    "github.com/AditiKulkarni9/clickhouse-flatfile-tool/services"
)

func UploadFlatFile(c *gin.Context) {
    file, err := c.FormFile("file")
    if err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": "File upload failed"})
        return
    }

    filePath := "./uploads/" + file.Filename
    if err := c.SaveUploadedFile(file, filePath); err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save file"})
        return
    }

    delimiter := c.PostForm("delimiter")
    if delimiter == "" {
        delimiter = ","
    }

    c.JSON(http.StatusOK, gin.H{"filePath": filePath, "delimiter": delimiter})
}

func GetFlatFileColumns(c *gin.Context) {
    filePath := c.Query("filePath")
    delimiter := c.Query("delimiter")
    if filePath == "" || delimiter == "" {
        c.JSON(http.StatusBadRequest, gin.H{"error": "Missing filePath or delimiter"})
        return
    }

    svc := services.NewFlatFileService(filePath, delimiter)
    columns, err := svc.GetColumns()
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        return
    }
    c.JSON(http.StatusOK, columns)
}
/*const handleTableSelect = async (table) => {
      setSelectedTable(table);
      setColumns([]);
      setSelectedColumns([]);
      setStatus({ message: 'Fetching columns...', type: 'loading' });
      try {
          let endpoint;
          if (sourceType === 'clickhouse') {
              endpoint = `http://localhost:8080/columns/clickhouse/${table}`;
          } else {
              endpoint = `http://localhost:8080/columns/flatfile?filePath=${flatFileConfig.filePath}&delimiter=${flatFileConfig.delimiter}`;
          }
          const res = await axios.get(endpoint);
          // Extract column names, whether from ClickHouse or flatfile
          const columnList = res.data.map(col => typeof col === 'string' ? col : col.name);
          setColumns(columnList);
          setStatus({ message: 'Columns loaded', type: 'success' });
      } catch (err) {
          setStatus({ message: `Error: ${err.response?.data?.error || err.message}`, type: 'error' });
      }
    };*/
