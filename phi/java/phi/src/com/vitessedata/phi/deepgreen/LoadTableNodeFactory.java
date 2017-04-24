package com.vitessedata.phi.deepgreen;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "WriteTable" Node.
 * 
 *
 * @author VitesseData
 */
public class LoadTableNodeFactory 
        extends NodeFactory<LoadTableNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public LoadTableNodeModel createNodeModel() {
        return new LoadTableNodeModel();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNrNodeViews() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeView<LoadTableNodeModel> createNodeView(final int viewIndex,
            final LoadTableNodeModel nodeModel) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasDialog() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeDialogPane createNodeDialogPane() {
        return new LoadTableNodeDialog();
    }

}

