namespace sap.capire.bookstore;

using {
    Currency,
    cuid,
    managed
} from '@sap/cds/common';

using {sap.capire.products.Products} from '@sap/capire-products';

entity Books as projection on Products;

extend Products with {
    author : Association to Authors;
}

entity Authors : cuid {
    firstName : String(11);
    lastName  : String(11);
    books     : Association to many Books
                    on books.author = $self;
}

@Capabilities.Updatable : false
entity Orders : cuid, managed {
    items    : Composition of many OrderItems
                   on items.parent = $self;
    total    : Decimal(9, 2) @readonly;
    currency : Currency;
}

@Capabilities.Updatable : false
entity OrderItems : cuid {
    parent    : Association to Orders not null;
    book_ID   : UUID;
    amount    : Integer;
    netAmount : Decimal(9, 2) @readonly;
}