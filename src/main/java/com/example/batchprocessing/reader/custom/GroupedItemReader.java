package com.example.batchprocessing.reader.custom;

import com.example.batchprocessing.model.Person;
import org.springframework.batch.item.*;
import org.springframework.batch.item.support.SingleItemPeekableItemReader;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;

public class GroupedItemReader<T> implements ItemReader<List<T>> {
    private final SingleItemPeekableItemReader<T> peekableItemReader;
    private int groupCounter = 0;
    private final ItemReader<T> itemReaderdelegate; // Your original ItemReader

    public GroupedItemReader(ItemReader<T> delegate) {
        Assert.notNull(delegate, "The 'itemReader' may not be null");
        this.peekableItemReader = new SingleItemPeekableItemReader<T>();

        this.itemReaderdelegate = delegate;
        this.peekableItemReader.setDelegate(itemReaderdelegate);
        this.peekableItemReader.open(new ExecutionContext());

    }

    @Override
    public List<T> read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        State state = State.NEW; // a simple enum with the states NEW, READING, and COMPLETE
        List<T> personList = null;
        T item = null;

        while (state != State.COMPLETE) {
            groupCounter++;
            item = peekableItemReader.read();

            switch (state) {

                case NEW: {
                    if (item == null) {
                        // end reached
                        state = State.COMPLETE;
                        break;
                    }

                    personList = new ArrayList<T>();
                    personList.add(item);
                    state = State.READING;
                    T nextItem = peekableItemReader.peek();
                    // isGroupBreak returns true, if 'item' and 'nextItem' do NOT belong to the same group
                    if (nextItem == null || isGroupBreak(item, nextItem)) {
                        state = State.COMPLETE;
                    }
                    break;
                }

                case READING: {
                    personList.add(item);

                    // peek and check if there the peeked entry has a new date
                    T nextItem = peekableItemReader.peek();
                    // isGroupBreak returns true, if 'item' and 'nextItem' do NOT belong to the same group
                    if (nextItem == null || isGroupBreak(item, nextItem)) {
                        state = State.COMPLETE;
                    }
                    break;
                }

                default: {
                    throw new org.springframework.expression.ParseException(groupCounter, "ParsingError: Reader is in an invalid state");
                }
            }
        }

        return personList;
    }

    private boolean isGroupBreak(T item, T nextItem) {
        Person currentPersonItem = (Person) item;
        Person nextPersonItem = (Person) nextItem;
        return currentPersonItem.id() != nextPersonItem.id();

    }

    public enum State {
        NEW,
        READING,
        COMPLETE;
    }
}
