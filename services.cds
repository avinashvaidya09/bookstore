using {sap.capire.bookstore as db} from './db/schema';
using {AdminService} from '@sap/capire-products';


service BookService {

    @readonly
    entity Books   as projection on db.Books {
        *,
        category as genre
    } excluding {
        category,
        createdBy,
        createdAt,
        modifiedBy,
        modifiedAt
    };

    @readonly
    entity Authors as projection on db.Authors;
}

service OrderService {
    entity Orders     as projection on db.Orders;
    entity OrderItems as projection on db.OrderItems;
}

extend service AdminService with {
    entity Authors as projection on db.Authors;
}
