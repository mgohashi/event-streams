'use strict';

var app = angular.module("dashboard", [])
    .controller("DashboardController", ["$scope", "$timeout", function ($scope, $timeout) {

        var opts2 = {
            angle: 0.1, // The span of the gauge arc
            lineWidth: 0.3, // The line thickness
            radiusScale: 1, // Relative radius
            pointer: {
                length: 0.6, // // Relative to gauge radius
                strokeWidth: 0.073, // The thickness
                color: '#000000' // Fill color
            },
            limitMax: false,     // If false, max value increases automatically if value > maxValue
            limitMin: false,     // If true, the min value of the gauge will be fixed
            colorStart: '#6FADCF',   // Colors
            colorStop: '#8FC0DA',    // just experiment with them
            strokeColor: '#E0E0E0',  // to see which ones work best for you
            generateGradient: true,
            highDpiSupport: true,     // High resolution support
            staticZones: [
                {strokeStyle: "#F03E3E", min: 0, max: 5}, // Red
                {strokeStyle: "#FFDD00", min: 5, max: 20}, // Yellow
                {strokeStyle: "#30B32D", min: 20, max: 100} // Green
            ],
            staticLabels: {
                font: "14px sans-serif",  // Specifies font
                labels: [0, 50, 100],  // Print labels at these values
                color: "#000000",  // Optional: Label text color
                fractionDigits: 0  // Optional: Numerical precision. 0=round off.
            }
        };

        var target2 = document.getElementById('gauge2'); // your canvas element
        $scope.gauge2 = new Gauge(target2).setOptions(opts2); // create sexy gauge!
        $scope.gauge2.maxValue = 100;
        $scope.gauge2.setMinValue(0);  // Prefer setter over gauge.minValue = 0
        $scope.gauge2.animationSpeed = 32; // set animation speed (32 is default value)
        $scope.gauge2.set(0); // set actual value

        var opts3 = {
            angle: 0.1, // The span of the gauge arc
            lineWidth: 0.3, // The line thickness
            radiusScale: 1, // Relative radius
            pointer: {
                length: 0.6, // // Relative to gauge radius
                strokeWidth: 0.073, // The thickness
                color: '#000000' // Fill color
            },
            limitMax: false,     // If false, max value increases automatically if value > maxValue
            limitMin: false,     // If true, the min value of the gauge will be fixed
            colorStart: '#6FADCF',   // Colors
            colorStop: '#8FC0DA',    // just experiment with them
            strokeColor: '#E0E0E0',  // to see which ones work best for you
            generateGradient: true,
            highDpiSupport: true,     // High resolution support
            staticZones: [
                {strokeStyle: "#30B32D", min: 0, max: 20}, // Green
                {strokeStyle: "#FFDD00", min: 20, max: 50}, // Yellow
                {strokeStyle: "#F03E3E", min: 50, max: 100} // Red
            ],
            staticLabels: {
                font: "14px sans-serif",  // Specifies font
                labels: [0, 20, 50, 100],  // Print labels at these values
                color: "#000000",  // Optional: Label text color
                fractionDigits: 0  // Optional: Numerical precision. 0=round off.
            }
        };

        var target3 = document.getElementById('gauge3'); // your canvas element
        $scope.gauge3 = new Gauge(target3).setOptions(opts3); // create sexy gauge!
        $scope.gauge3.maxValue = 100;$scope.eventBus = new EventBus('http://localhost:8080/eventbus');
        $scope.gauge3.setMinValue(0);  // Prefer setter over gauge.minValue = 0$scope.eventBus.enableReconnect(true);
        $scope.gauge3.animationSpeed = 32; // set animation speed (32 is default value)
        $scope.gauge3.set(0); // set actual value

        $scope.eventBus.onreconnect = function () {
            console.log("Event bus reconnected!")
        };

        $scope.amountCoffee = 0;
        $scope.amountMilk = 0;
        $scope.amountBread = 0;

        $scope.eventBus.onopen = function () {
            console.log("Event bus connected!");
            $scope.eventBus.registerHandler('public-orders-placed', function (error, message) {
                if (error === null) {
                    $scope.$apply(function () {
                        var count = JSON.parse(message.body).count;
                        $scope.count = count;
                    });
                } else {
                    console.log(error);
                }
            });
            $scope.eventBus.registerHandler('public-orders-delivered', function (error, message) {
                if (error === null) {
                    $scope.$apply(function () {
                        var percent = JSON.parse(message.body).percent;
                        $scope.gauge2.set(percent);
                    });
                } else {
                    console.log(error);
                }
            });
            $scope.eventBus.registerHandler('public-orders-cancelled', function (error, message) {
                if (error === null) {
                    $scope.$apply(function () {
                        var percent = JSON.parse(message.body).percent;
                        $scope.gauge3.set(percent);
                    });
                } else {
                    console.log(error);
                }
            });
            $scope.eventBus.registerHandler('public-product-count', function (error, message) {
                if (error === null) {
                    $scope.$apply(function () {
                        var products = JSON.parse(message.body);
                        for (var key in products) {
                            var product = products[key];
                            if (product.id === '1') {
                                $scope.amountCoffee = product.amount;
                            } else if (product.id === '2') {
                                $scope.amountMilk = product.amount;
                            } else if (product.id === '3') {
                                $scope.amountBread = product.amount;
                            }
                        }
                    });
                } else {
                    console.log(error);
                }
            });
        };

    }]);



