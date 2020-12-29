package com.github.sparktacus.sparknlp.spark.nlp.app.utils;

import java.util.Iterator;

public interface CloseableIterator<E> extends Iterator<E>, AutoCloseable {

}
