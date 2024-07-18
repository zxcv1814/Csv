const fs = require('fs');
const fastcsv = require('fast-csv');
const { MongoClient } = require('mongodb');

const url = "mongodb+srv://dongquang:HWGxBa7ni6UVE6JY@table.mc0oyvt.mongodb.net/";
const dbName = 'table';

const mongoClient = new MongoClient(url);

async function importCSVToMongoDB(csvFilePath) {
    try {
        await mongoClient.connect();
        console.log("Kết nối thành công đến MongoDB");

        const db = mongoClient.db(dbName);
        const collection = db.collection('giftcards');

        let csvData = [];
        fs.createReadStream(csvFilePath)
            .pipe(fastcsv.parse({ headers: true }))
            .on('data', (row) => {
                csvData.push({
                    "_id": row._id,
                    "giftCardId": row.giftCardId,
                    "emailSent": (row.emailSent.toLowerCase() === 'true'),
                    "email": row.email,
                    "name": row.name,
                    "date": row.date,
                    "timestamp": row.timestamp,
                    "timezone": row.timezone,
                    "code": row.code,
                    "productImage": row.productImage,
                    "productTitle": row.productTitle,
                    "user_id": row.user_id,
                    "user": row.user,
                    "balance": row.balance,
                    "currency": row.currency,
                    "note": row.note,
                    "hashedCode": row.hashedCode,
                    "locale": row.locale,
                    "isDisabled": (row.isDisabled.toLowerCase() === 'true'),
                    "orderFulFill": (row.orderFulFill.toLowerCase() === 'true'),
                    "isPhysicalGiftCard": (row.isPhysicalGiftCard.toLowerCase() === 'true'),
                    "createdAt": new Date(row.createdAt),
                    "updatedAt": new Date(row.updatedAt),
                    "__v": row.__v,
                });
            })
            .on('end', async () => {
                try {
                    const result = await collection.insertMany(csvData);
                    console.log(`Đã import ${result.insertedCount} bản ghi thành công`);
                } catch (error) {
                    console.error("Lỗi khi chèn dữ liệu:", error);
                } finally {
                    await mongoClient.close();
                    console.log("Đã đóng kết nối tới MongoDB");
                }
            });
    } catch (error) {
        console.error("Lỗi:", error);
    }
}

const csvFilePath = 'C:\\Users\\quang\\OneDrive\\Desktop\\table\\csv.csv';
importCSVToMongoDB(csvFilePath);
