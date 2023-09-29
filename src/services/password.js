const isValidPassword = (password, endpoint) => {
    if (endpoint === '/api/billing/return-balance') {
        return (password === process.env.BILLING_RETURN_BALANCE_PASSWORD);
    } else if (endpoint === '/api/billing/remove-balance-api') {
        return (password === process.env.BILLING_REMOVE_BALANCE_API_PASSWORD);
    }
    return false;
}

module.exports = {
    isValidPassword
};
