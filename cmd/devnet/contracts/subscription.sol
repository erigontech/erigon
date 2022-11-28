// SPDX-License-Identifier: GPL-3.0

pragma solidity ^0.8.0;

contract Subscription {
    event SubscriptionEvent();
    fallback() external {
        emit SubscriptionEvent();
    }
}
