package com.vitessedata.phi.deepgreen;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.knime.base.node.io.csvwriter.CSVWriter;
import org.knime.base.node.io.csvwriter.FileWriterSettings;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.IntValue;
import org.knime.core.data.date.DateAndTimeValue;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.database.DatabaseConnectionPortObject;
import org.knime.core.node.port.database.DatabaseConnectionPortObjectSpec;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.streamable.DataTableRowInput;
import org.knime.core.node.streamable.InputPortRole;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

/**
 * This is the model implementation of WriteTable.
 * 
 *
 * @author VitesseData
 */
public class LoadTableNodeModel extends NodeModel {
    
    // the logger instance
    // private static final NodeLogger logger = NodeLogger.getLogger(LoadTableNodeModel.class);
    // private String m_errmsg = null;
    
    /** Config key for the table name. */
    static final String KEY_TABLE_NAME = "table";
    private String m_tableName;
    
    /** Config key for the append data. */
    static final String KEY_APPEND_DATA = "append_data";
    private boolean m_append = true;
    
    /** Config key for Append Optimized Storage */
    static final String KEY_STORAGE_AO = "ao";
    private boolean m_ao = true;
    
    /** Config key for Append Optimize/Column Store */
    static final String KEY_STORAGE_CS = "cs";
    private boolean m_cs = true;
    
    /** Config key for compression type */
    static final String KEY_STORAGE_COMPTYPE = "compression_type";
    private String m_comptype;
    
    /** Config key for distribution key */
    static final String KEY_DISTRIBUTED_BY = "distributed_by";
    private String m_distributedby;
    private final Map<String, String> m_types =
            new LinkedHashMap<String, String>();

    /** Default SQL-type for Strings. */
    static final String SQL_TYPE_STRING = "text"; 

    /** Default SQL-type for Booleans. */
    static final String SQL_TYPE_BOOLEAN = "boolean";

    /** Default SQL-type for Integers. */
    static final String SQL_TYPE_INTEGER = "integer";

    /** Default SQL-type for Doubles. */
    static final String SQL_TYPE_DOUBLE = "double precision";

    /** Default SQL-type for Timestamps. */
    static final String SQL_TYPE_DATEANDTIME = "timestamp";

    /** Default SQL-type for Date. */
    static final String SQL_TYPE_BLOB = "bytea"; 

    /** Config key for column to SQL-type mapping. */
    static final String CFG_SQL_TYPES = "sql_types";
 

    /**
     * Constructor for the node model.
     */
    protected LoadTableNodeModel() {
        super(new PortType[]{BufferedDataTable.TYPE, DatabaseConnectionPortObject.TYPE}, new PortType[0]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addString(KEY_TABLE_NAME, m_tableName);
        settings.addBoolean(KEY_APPEND_DATA, m_append);
        settings.addBoolean(KEY_STORAGE_AO, m_ao);
        settings.addBoolean(KEY_STORAGE_CS, m_cs);
        settings.addString(KEY_STORAGE_COMPTYPE, m_comptype); 
        settings.addString(KEY_DISTRIBUTED_BY, m_distributedby); 

        // save SQL Types mapping
        NodeSettingsWO typeSett = settings.addNodeSettings(CFG_SQL_TYPES);
        for (Map.Entry<String, String> e : m_types.entrySet()) {
            typeSett.addString(e.getKey(), e.getValue());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
    	loadSettings(settings, false);
        // TODO check if the settings could be applied to our model

    }
 

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {
    	loadSettings(settings, true);
    }
    
    private void loadSettings(final NodeSettingsRO settings, final boolean write)
    				throws InvalidSettingsException {
    	boolean append = settings.getBoolean(KEY_APPEND_DATA, true);
    	final String table = settings.getString(KEY_TABLE_NAME);
    	if (table == null || table.trim().isEmpty()) {
    		throw new InvalidSettingsException("Configure node and enter a valid table name.");
    	}
    	
    	boolean ao = settings.getBoolean(KEY_STORAGE_AO, true);
    	boolean cs = settings.getBoolean(KEY_STORAGE_CS, true);
    	final String comptype = settings.getString(KEY_STORAGE_COMPTYPE);
    	if (!ao) {
    		if (cs) {
    			throw new InvalidSettingsException("Columns store can only be used with AO table.");
    		}
    		
    		if (comptype != null && (!(comptype.trim().equals("") || comptype.trim().equals("none")))) { 
    			throw new InvalidSettingsException("Compression type can only be used with AO table.");
    		}
    	}
    	
    	final String distributedby = settings.getString(KEY_DISTRIBUTED_BY);
    	
    	if (write) {
    		m_tableName = table;
    		m_append = append;
    		m_ao = ao;
    		m_cs = cs;
    		m_comptype = comptype;
    		m_distributedby = distributedby;
    		
    		try {
    			NodeSettingsRO typeSett = settings.getNodeSettings(CFG_SQL_TYPES);
    			for (String key : typeSett.keySet()) { 
    				m_types.put(key,  typeSett.getString(key));
    			}
    		} catch (InvalidSettingsException e) {
    			// ignore...
    		}
    	}
    }
    
    private String createTableSql(DataTableSpec tableSpec) {
    	StringBuilder sb = new StringBuilder(); 
    	sb.append("CREATE TABLE ").append(m_tableName).append(" ( \n");
    	
    	// Columns, all nullable, no primary key, 
    	for (int i = 0; i < tableSpec.getNumColumns(); i++) {
    		final String name = tableSpec.getColumnSpec(i).getName();
    		String sqlType = m_types.get(name);
    		if (sqlType == null) {
    			final DataType dt = tableSpec.getColumnSpec(i).getType();
    			if (dt.isCompatible(IntValue.class)) {
    				sqlType = SQL_TYPE_INTEGER;
    			} else if (dt.isCompatible(DoubleValue.class)) {
    				sqlType = SQL_TYPE_DOUBLE; 
    			} else if (dt.isCompatible(DateAndTimeValue.class)) {
    				sqlType = SQL_TYPE_DATEANDTIME; 
    			} else {
    				sqlType = SQL_TYPE_STRING;
    			}
    		}
    		sb.append(name).append(" ").append(sqlType);
    		if (i + 1 == tableSpec.getNumColumns()) {
    			sb.append(") \n");
    		} else {
    			sb.append(", \n");
    		}
    	}
    	
    	if (m_ao) {
    		sb.append("WITH ( APPENDONLY=TRUE ");
    		if (m_cs) {
    			sb.append(", ORIENTATION=COLUMN ");
    		} else {
    			sb.append(", ORIENTATION=ROW ");
    		}
    		if (m_comptype != null && !m_comptype.trim().isEmpty()) {
    			sb.append(", COMPTYPE=").append(m_comptype);
    		}
    		sb.append(")\n");
    	}
    	
    	if (m_distributedby == null || m_distributedby.trim().isEmpty()) {
    		// Would rather do random distribution instead of let dg guess one.  At least it will not skew.
    		sb.append(" DISTRIBUTED RANDOMLY\n");
    	} else {
    		sb.append(" DISTRIBUTED BY (").append(m_distributedby).append(")\n");
    	}

    	return sb.toString();
    }
    	
    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inData, 
            final ExecutionContext exec) throws Exception {
    	BufferedDataTable inTable = (BufferedDataTable) inData[0];
    	DataTableRowInput rowInput = new DataTableRowInput(inTable);
    	DataTableSpec inSpec = inTable.getDataTableSpec();
    	
    	// Get a raw connection using info stored in connection port.  We do not
    	// use DatabaseConnectionSettings.createConnection, because it will return
    	// a proxy or something that wraps postgres connection.   This will cause 
    	// big trouble when we try to get a CopyManager.
    	DatabaseConnectionPortObject connport = (DatabaseConnectionPortObject ) inData[1];
    	CredentialsProvider cp = getCredentialsProvider();
    	DatabaseConnectionSettings connSettings = connport.getConnectionSettings(cp); 
    	Connection conn = DGUtil.rawConn(connSettings, cp); 
    	
    	int tableExists = 1;
    	try {
    		// Test if table exists actually is not trivial -- if table schema is not given
    		// we need to take care of search_path.  Hack with try/catch.
    		DGUtil.execQueryInt(conn, "select 1 from " + m_tableName + " where false");
    	} catch (Exception e) {
    		tableExists = 0;
    	}
    	
    	try {
    		// Later the queries, if fail, let it throw.   We put everything in this 
    		// try catch block so that we can close connection.
    		if (tableExists == 1) {
    			if (!m_append) {
    				DGUtil.execQuery(conn,  "Drop table " + m_tableName);
    				tableExists = 0;
    			}
    		}

    		if (tableExists == 0) {
    			// Create table.
    			String createSql = createTableSql(inSpec);
    			DGUtil.execQuery(conn,  createSql);
    		}

    		// Next implement data load using copy. 
    		CopyManager copyManager = new CopyManager((BaseConnection) conn);

    		// Seems default setting is just fine.   First write to StringWriter
    		FileWriterSettings writerSettings = new FileWriterSettings(); 
    		StringWriter w = new StringWriter();
    		CSVWriter tableWriter = new CSVWriter(w, writerSettings);
    		tableWriter.write(rowInput, exec);
    		tableWriter.close();

    		// Convert to a StringReader, pass to copy manager as stdin.
    		StringReader r = new StringReader(w.toString());
    		copyManager.copyIn("COPY " + m_tableName + " FROM STDIN CSV", r); 

    		if (!conn.getAutoCommit()) {
    			conn.commit();
    		}
    	} catch (Exception e) {
    		conn.close();
    		throw e;
    	}
    	
    	conn.close();
    	return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputPortRole[] getInputPortRoles() {
        return new InputPortRole[]{InputPortRole.NONDISTRIBUTED_STREAMABLE, InputPortRole.NONDISTRIBUTED_NONSTREAMABLE};
    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
    	// Noop
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs)
            throws InvalidSettingsException {
    	DataTableSpec tableSpec = (DataTableSpec) inSpecs[0];
    	DatabaseConnectionPortObjectSpec connSpec = (DatabaseConnectionPortObjectSpec) inSpecs[1];
    	DatabaseConnectionSettings connSettings = connSpec.getConnectionSettings(getCredentialsProvider());
    	
    	if (connSettings.getJDBCUrl() == null || connSettings.getJDBCUrl().isEmpty()) {
    		throw new InvalidSettingsException("No database connection info.");
    	}
    	
    	if (m_tableName == null || m_tableName.trim().isEmpty()) {
    		throw new InvalidSettingsException("Configure node and enter a valid table name.");
    	}
    	
    	if (tableSpec.getNumColumns() == 0) {
    		throw new InvalidSettingsException("Input data does not have any column."); 
    	}
    	
    	// Get table schema.
    	Map<String, String> colmap = new LinkedHashMap<String, String>();
    	for (int i = 0; i < tableSpec.getNumColumns(); i++) {
    		final String name = tableSpec.getColumnSpec(i).getName();
    		String sqlType = m_types.get(name);
    		if (sqlType == null) {
    			final DataType dt = tableSpec.getColumnSpec(i).getType();
    			if (dt.isCompatible(IntValue.class)) {
    				sqlType = SQL_TYPE_INTEGER;
    			} else if (dt.isCompatible(DoubleValue.class)) {
    				sqlType = SQL_TYPE_DOUBLE; 
    			} else if (dt.isCompatible(DateAndTimeValue.class)) {
    				sqlType = SQL_TYPE_DATEANDTIME; 
    			} else {
    				sqlType = SQL_TYPE_STRING;
    			}
    		}
    		colmap.put(name,  sqlType);
    	}
    	
    	m_types.clear();
    	m_types.putAll(colmap);
    	
    	if (!m_append) {
    		super.setWarningMessage("Existing table \"" + m_tableName + "\" will be dropped!");
    	}
    	return new DataTableSpec[0];
    }
   
    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File internDir,
            final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException {
    	// Noop.

    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File internDir,
            final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException {
    	// Noop
    }
}

