package com.sap.cap.bookstore.handlers;

import java.math.BigDecimal;
import java.util.List;

import javax.sql.rowset.serial.SerialException;

import com.sap.cds.ql.Select;
import com.sap.cds.ql.Update;
import com.sap.cds.ql.cqn.CqnSelect;
import com.sap.cds.ql.cqn.CqnUpdate;
import com.sap.cds.services.ErrorStatuses;
import com.sap.cds.services.ServiceException;
import com.sap.cds.services.cds.CdsService;
import com.sap.cds.services.handler.EventHandler;
import com.sap.cds.services.handler.annotations.After;
import com.sap.cds.services.handler.annotations.Before;
import com.sap.cds.services.handler.annotations.ServiceName;
import com.sap.cds.services.persistence.PersistenceService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import cds.gen.bookservice.Books;
import cds.gen.bookservice.Books_;
import cds.gen.orderservice.OrderItems;
import cds.gen.orderservice.Orders;
import cds.gen.sap.capire.bookstore.OrderItems_;

@Component
@ServiceName("OrderService")
public class OrderService implements EventHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(OrderService.class);

    @Autowired
    PersistenceService db;

    @Before(event = CdsService.EVENT_CREATE, entity = "OrderService.OrderItems")
    public void validateBookAndDecreaseStock(List<OrderItems> items) throws SerialException {

        for (OrderItems item : items) {

            String bookId = item.getBookId();
            Integer quantity = item.getAmount();

            CqnSelect select = Select.from(Books_.class).columns(b -> b.stock()).where(b -> b.ID().eq(bookId));

            Books book = db.run(select).first(Books.class).orElseThrow(() -> new SerialException("Book not found"));

            // check if the order could be fulfilled
            int stock = book.getStock();

            if (stock < quantity.intValue()) {
                throw new ServiceException(ErrorStatuses.BAD_REQUEST, "Not enough stocks");
            }

            book.setStock(stock - quantity);

            try {
                CqnUpdate update = Update.entity(Books_.class).data(book).where(b -> b.ID().eq(bookId));
                db.run(update);

            } catch (Exception e) {
                LOGGER.info("Error in updating stocks for book", e);
            }

        }

    }

    @Before(event = CdsService.EVENT_CREATE, entity = "OrderService.Orders")
    public void validateBookAndDecreaseStockViaOrders(List<Orders> orders) {
        for (Orders order : orders) {
            if (order.getItems() != null) {
                try {
                    validateBookAndDecreaseStock(order.getItems());
                } catch (SerialException e) {
                    LOGGER.info("Error in validateBookAndDecreaseStockViaOrders", e);
                }
            }
        }
    }

    @After(event = { CdsService.EVENT_READ, CdsService.EVENT_CREATE }, entity = "OrderService.OrderItems")
    public void calculateNetAmount(List<OrderItems> items) {
        for (OrderItems item : items) {
            String bookId = item.getBookId();

            // get the book that was ordered
            CqnSelect sel = Select.from(Books_.class).where(b -> b.ID().eq(bookId));
            Books book = db.run(sel).single(Books.class);

            // calculate and set net amount
            item.setNetAmount(book.getPrice().multiply(new BigDecimal(item.getAmount())));
        }
    }

    @After(event = { CdsService.EVENT_READ, CdsService.EVENT_CREATE }, entity = "OrderService.Orders")
    public void calculateTotal(List<Orders> orders) {
        for (Orders order : orders) {
            // calculate net amount for expanded items
            if(order.getItems() != null) {
                calculateNetAmount(order.getItems());
            }

            // get all items of the order
            CqnSelect selItems = Select.from(OrderItems_.class).where(i -> i.parent().ID().eq(order.getId()));
            List<OrderItems> allItems = db.run(selItems).listOf(OrderItems.class);

            // calculate net amount of all items
            calculateNetAmount(allItems);

            // calculate and set the orders total
            BigDecimal total = new BigDecimal(0);
            for(OrderItems item : allItems) {
                total = total.add(item.getNetAmount());
            }
            order.setTotal(total);
        }
    }

}
