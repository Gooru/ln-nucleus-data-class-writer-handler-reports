package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.transactions;

/**
 * This package provides a convenience class which is used to run the DBHandlers against the Database using
 * active jdbc methods of getting connections and running transactions.
 * It provides try - catch handlers to make sure that handlers when run are either committed or rolled back,
 * and the connection is detached from current thread. In case there was an operation marking connection as
 * read only, it needs to be restored back to its value
 */
