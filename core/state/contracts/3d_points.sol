// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

contract Point3DFactory {
    // Array to store all deployed point contracts
    address[] public deployedPoints;

    // Mapping from point ID to contract address
    mapping(int256 => address) public pointById;

    // Counter for point IDs
    int256 public nextPointId = 1;

    // Events
    event PointDeployed(
        address indexed pointAddress,
        int256 indexed pointId,
        int256 x,
        int256 y,
        int256 z
    );

    function deployNextPoint() external returns (address) {
        // Create new Point3D contract
        int256 x = nextPointId * 2;
        int256 y = nextPointId * 3;
        int256 z = nextPointId * 4;
        Point3D newPoint = new Point3D(x, y, z, nextPointId);

        // Store the contract address
        address pointAddress = address(newPoint);
        deployedPoints.push(pointAddress);
        pointById[nextPointId] = pointAddress;

        // Emit event
        emit PointDeployed(pointAddress, newPoint.pointId(), newPoint.x(), newPoint.y(), newPoint.z());

        // Increment point ID for next deployment
        nextPointId++;

        return pointAddress;
    }

    function getDeployedPoints() external view returns (address[] memory) {
        return deployedPoints;
    }

    function getPointAddress(int256 _pointId) external view returns (address) {
        return pointById[_pointId];
    }

    function getTotalPoints() external view returns (uint256) {
        return deployedPoints.length;
    }

    function getPointCoordinates(int256 _pointId) external view returns (int256, int256, int256) {
        address pointAddress = pointById[_pointId];
        require(pointAddress != address(0), "Point does not exist");

        Point3D point = Point3D(pointAddress);
        return point.getCoordinates();
    }

    function getAllPointsData() external view returns (
        address[] memory addresses,
        int256[] memory pointIds,
        int256[] memory xCoords,
        int256[] memory yCoords,
        int256[] memory zCoords
    ) {
        uint256 length = deployedPoints.length;

        addresses = new address[](length);
        pointIds = new int256[](length);
        xCoords = new int256[](length);
        yCoords = new int256[](length);
        zCoords = new int256[](length);

        for (uint256 i = 0; i < length; i++) {
            Point3D point = Point3D(deployedPoints[i]);
            addresses[i] = deployedPoints[i];
            pointIds[i] = point.pointId();
            (xCoords[i], yCoords[i], zCoords[i]) = point.getCoordinates();
        }
    }
}

contract Point3D {
    int256 public x;
    int256 public y;
    int256 public z;
    address public factory;
    int256 public pointId;

    constructor(int256 _x, int256 _y, int256 _z, int256 _pointId) {
        x = _x;
        y = _y;
        z = _z;
        factory = msg.sender;
        pointId = _pointId;
    }

    function getCoordinates() external view returns (int256, int256, int256) {
        return (x, y, z);
    }

    function getPointInfo() external view returns (int256, int256, int256, int256, address) {
        return (x, y, z, pointId, factory);
    }
}
