package com.vitessedata.phi.deepgreen;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;

import javax.swing.BorderFactory;
import javax.swing.JCheckBox;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;

import org.knime.base.node.io.database.DBSQLTypesPanel;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;

/**
 * Dialog pane of the deepgreen table writer.
 *
 * @author Feng Tian, VitesseData Inc.
 */
final class LoadTableNodeDialog extends NodeDialogPane {

    private final JTextField m_table = new JTextField("");
    private final JCheckBox m_append = new JCheckBox("... to existing table (if any!)");
    private final JCheckBox m_ao = new JCheckBox("append optimized storage");
    private final JCheckBox m_cs = new JCheckBox("column store");
    private final JTextField m_comptype = new JTextField("");
    private final JTextField m_distributedby = new JTextField("");
    private final DBSQLTypesPanel m_typePanel;


    /**
     * Creates new dialog.
     */
    LoadTableNodeDialog() {
        // add login and table name tab
        JPanel tableAndConnectionPanel = new JPanel(new GridBagLayout());

        GridBagConstraints c = new GridBagConstraints();
        c.gridx = 0;
        c.gridy = 0;
        c.anchor = GridBagConstraints.NORTHWEST;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 1;

        JPanel p = new JPanel(new GridLayout());
        p.add(m_table);
        p.setBorder(BorderFactory.createTitledBorder(" Table Name "));
        tableAndConnectionPanel.add(p, c);
        c.gridy++;
        
        p = new JPanel(new GridLayout());
        p.add(m_distributedby);
        p.setBorder(BorderFactory.createTitledBorder(" Distributed by "));
        tableAndConnectionPanel.add(p, c);
        c.gridy++;
        
        p = new JPanel(new GridLayout());
        p.add(m_append);
        p.setBorder(BorderFactory.createTitledBorder(" Append Data "));
        tableAndConnectionPanel.add(p, c);
        c.gridy++;
        
        p = new JPanel(new GridLayout());
        p.add(m_ao);
        p.add(m_cs);
        p.setBorder(BorderFactory.createTitledBorder(" Storage Options "));
        tableAndConnectionPanel.add(p, c);
        c.gridy++;
        
        p = new JPanel(new GridLayout());
        p.add(m_comptype);
        p.setBorder(BorderFactory.createTitledBorder(" Compression Method "));
        tableAndConnectionPanel.add(p, c);
        c.gridy++;
        super.addTab("Settings", tableAndConnectionPanel);

        // add SQL Types tab
        m_typePanel = new DBSQLTypesPanel();
        final JScrollPane scroll = new JScrollPane(m_typePanel);
        super.addTab("SQL Types", scroll);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings,
            final PortObjectSpec[] specs) throws NotConfigurableException {
        m_table.setText(settings.getString(LoadTableNodeModel.KEY_TABLE_NAME, ""));
        m_append.setSelected(settings.getBoolean(LoadTableNodeModel.KEY_APPEND_DATA, true));
        m_ao.setSelected(settings.getBoolean(LoadTableNodeModel.KEY_STORAGE_AO, true));
        m_cs.setSelected(settings.getBoolean(LoadTableNodeModel.KEY_STORAGE_CS, true));
        m_comptype.setText(settings.getString(LoadTableNodeModel.KEY_STORAGE_COMPTYPE, ""));
        m_distributedby.setText(settings.getString(LoadTableNodeModel.KEY_DISTRIBUTED_BY, ""));

        // load SQL Types for each column
        try {
            NodeSettingsRO typeSett = settings.getNodeSettings(LoadTableNodeModel.CFG_SQL_TYPES);
            m_typePanel.loadSettingsFrom(typeSett, (DataTableSpec)specs[0]);
        } catch (InvalidSettingsException ise) {
            m_typePanel.loadSettingsFrom(null, (DataTableSpec)specs[0]);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings)
            throws InvalidSettingsException {
        settings.addString(LoadTableNodeModel.KEY_TABLE_NAME, m_table.getText().trim());
        settings.addBoolean(LoadTableNodeModel.KEY_APPEND_DATA, m_append.isSelected());
        settings.addBoolean(LoadTableNodeModel.KEY_STORAGE_AO,  m_ao.isSelected());
        settings.addBoolean(LoadTableNodeModel.KEY_STORAGE_CS,  m_cs.isSelected());
        settings.addString(LoadTableNodeModel.KEY_STORAGE_COMPTYPE, m_comptype.getText().trim());
        settings.addString(LoadTableNodeModel.KEY_DISTRIBUTED_BY, m_distributedby.getText().trim());

        // save SQL Types for each column
        NodeSettingsWO typeSett = settings.addNodeSettings(LoadTableNodeModel.CFG_SQL_TYPES);
        m_typePanel.saveSettingsTo(typeSett);
    }
}
