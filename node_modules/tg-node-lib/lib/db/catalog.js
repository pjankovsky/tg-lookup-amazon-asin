'use strict';
let config = require('tg-node-lib/lib/config').getAllConfig();
let Sequelize = require('sequelize');
let db = new Sequelize(config['db.product.database'], config['db.product.user'], config['db.product.password'], {
    host: config['db.product.host'],
    dialect: 'mysql',
    pool: {
        maxConnections: 50,
        minConnections: 0,
        maxIdleTime: 10000
    }
});

let Product = db.define('product', {
    timestamps: true,
    id: {
        type: Sequelize.INTEGER.UNSIGNED,
        primaryKey: true
    },
    asin: {
        type: Sequelize.STRING(10),
        allowNull: false,
        unique: true
    },
    parentAsin: {
        type: Sequelize.STRING(10),
        allowNull: true
    },
    title: {
        type: Sequelize.STRING,
        allowNull: false
    },
    affiliateLink: {
        type: Sequelize.STRING,
        allowNull: false
    },
    salesRank: {
        type: Sequelize.BIGINT.UNSIGNED,
        allowNull: false
    },
    brand: {
        type: Sequelize.STRING,
        allowNull: false
    },
    ean: {
        type: Sequelize.STRING(13),
        allowNull: true
    },
    mpn: {
        type: Sequelize.STRING,
        allowNull: false
    },
    description: {
        type: Sequelize.TEXT('medium'),
        allowNull: false
    },
    features: {
        type: Sequelize.TEXT('medium'),
        allowNull: false
    },
    boxWidth: {
        type: Sequelize.INTEGER.UNSIGNED,
        allowNull: false,
        default: 1000,
        comment: 'hundredths-inches'
    },
    boxLength: {
        type: Sequelize.INTEGER.UNSIGNED,
        allowNull: false,
        default: 1000,
        comment: 'hundredths-inches'
    },
    boxHeight: {
        type: Sequelize.INTEGER.UNSIGNED,
        allowNull: false,
        default: 1000,
        comment: 'hundredths-inches'
    },
    boxWeight: {
        type: Sequelize.INTEGER.UNSIGNED,
        allowNull: false,
        default: 1000,
        comment: 'hundredths-pounds'
    },
    productGroup: {
        type: Sequelize.STRING,
        allowNull: true
    },
    productType: {
        type: Sequelize.STRING,
        allowNull: true
    }
}, {
    instanceMethods: {
        importAmazonItem: (item) => {
            console.log(item);
        }
    }
});

let ProductImage = db.define('product_image', {
    timestamps: true,
    id: {
        type: Sequelize.INTEGER.UNSIGNED,
        primaryKey: true
    },
    index: {
        type: Sequelize.INTEGER.UNSIGNED,
        allowNull: false
    },
    url: {
        type: Sequelize.TEXT,
        allowNull: false
    }
}, {});

Product.hasMany(ProductImage, {as: 'Images'});

let ProductPrices = db.define('product_price', {
    timestamps: true,
    id: {
        type: Sequelize.INTEGER.UNSIGNED,
        primaryKey: true
    },
    msrp: {
        type: Sequelize.INTEGER.UNSIGNED,
        allowNull: true,
        comment: 'hundredths-dollars'
    },
    cost: {
        type: Sequelize.INTEGER.UNSIGNED,
        allowNull: false,
        comment: 'hundredths-dollars'
    },
    price: {
        type: Sequelize.INTEGER.UNSIGNED,
        allowNull: false,
        comment: 'hundredths-dollars'
    }
}, {});

Product.hasOne(ProductPrices, {as: 'Prices'});

let ProductCategory = db.define('product_category', {
    timestamps: true,
    id: {
        type: Sequelize.INTEGER.UNSIGNED,
        primaryKey: true
    },
    isPrimary: {
        type: Sequelize.BOOLEAN,
        allowNull: false,
        default: 0
    },
    amazonCategory: {
        type: Sequelize.TEXT,
        allowNull: false
    },
    searsCategoryId: {
        type: Sequelize.INTEGER.UNSIGNED,
        allowNull: true,
        comment: 'Index column. May not always be accurate.'
    }
}, {});

Product.hasMany(ProductCategory, {as: 'Categories'});

module.exports = {
    setup: () => {
        return Promise.all([
            Product.sync(),
            ProductImage.sync(),
            ProductCategory.sync(),
            ProductPrices.sync()
        ]);
    },
    Product: () => {
        return Product;
    },
    ProductImage: () => {
        return ProductImage;
    },
    ProductCategory: () => {
        return ProductCategory;
    },
    ProductPrices: () => {
        return ProductPrices;
    },
    importAmazonItem: (item) => {
        console.log(JSON.stringify(item));
        // always return a promise
        return Product.count({where: {asin: item.ASIN}})
            .then((count) => {
                if (count == 1) {
                    // update product
                    return Product.fineOne({where: {asin: item.ASIN}});
                } else {
                    // create product
                    return new Promise((resolve) => resolve(Product.build({asin: item.ASIN})));
                }
            })
            .then((product) => {
                if (product === null) // findOne returns null for no rows
                    return false; // break
                product.importAmazonItem(item);
                return false;
            });
    }
};
