SELECT
   id,
    orderid as order_id,
    paymentmethod as payment_method,
    status,
    amount
    

FROM raw.stripe.payment