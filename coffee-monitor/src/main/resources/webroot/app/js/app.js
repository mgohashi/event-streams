'use strict';

var app = angular.module("dashboard", [])
    .controller("DashboardController", ["$scope", "$timeout", function ($scope, $timeout) {

        function createChart(ctx, suggestMax, data) {
            return new Chart(ctx, {
                type: 'line',
                data: {
                    labels: ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"],
                    datasets: [{
                        label: "My First dataset",
                        data: data,
                        fill: true,
                        backgroundColor: "#80b3ff",
                        borderColor: "#0066ff",
                        borderCapStyle: 'butt',
                        borderDash: [0, 0]
                    }]
                },
                options: {
                    legend: {
                        display: false
                    },
                    scales: {
                        yAxes: [{
                            display: true,
                            ticks: {
                                suggestedMin: 0,
                                suggestedMax: suggestMax
                            }
                        }],
                        xAxes: [{
                            display: true
                        }]
                    }
                }
            });
        }

        function createGauge(elementId, staticZones, maxValue) {
            var opts = {
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
                staticZones: staticZones,
                staticLabels: {
                    font: "14px sans-serif",  // Specifies font
                    labels: [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100],  // Print labels at these values
                    color: "#000000",  // Optional: Label text color
                    fractionDigits: 0  // Optional: Numerical precision. 0=round off.
                }
            };

            var target = document.getElementById(elementId); // your canvas element

            var gauge = new Gauge(target).setOptions(opts); // create sexy gauge!
            gauge.maxValue = maxValue;
            gauge.setMinValue(0);  // Prefer setter over gauge.minValue = 0
            gauge.animationSpeed = 32; // set animation speed (32 is default value)
            gauge.set(0); // set actual value

            return gauge;
        }

        var staticZones1 = [
            {strokeStyle: "#F03E3E", min: 0, max: 5}, // Red
            {strokeStyle: "#FFDD00", min: 5, max: 20}, // Yellow
            {strokeStyle: "#30B32D", min: 20, max: 100} // Green
        ];

        $scope.gauge2 = createGauge('gauge2', staticZones1, 100);

        var staticZones2 = [
            {strokeStyle: "#30B32D", min: 0, max: 20}, // Green
            {strokeStyle: "#FFDD00", min: 20, max: 50}, // Yellow
            {strokeStyle: "#F03E3E", min: 50, max: 100} // Red
        ];

        $scope.gauge3 = createGauge('gauge3', staticZones2, 100);

        $scope.eventBus = new EventBus('http://localhost:8080/eventbus');

        $scope.eventBus.onreconnect = function () {
            console.log("Event bus reconnected!")
        };

        var coffeeCtx = document.getElementById('coffeeChart');

        $scope.coffeeChart = createChart(coffeeCtx, 1000, []);

        var milkCtx = document.getElementById('milkChart');

        $scope.milkChart = createChart(milkCtx, 1000, []);

        var breadCtx = document.getElementById('breadChart');

        $scope.breadChart = createChart(breadCtx, 10, []);

        var optCounter = {
            startVal: 0
        };

        $scope.counter = new CountUp('counter', 0, optCounter);

        $scope.eventBus.onopen = function () {
            console.log("Event bus connected!");
            $scope.eventBus.registerHandler('public-orders-placed', function (error, message) {
                if (error === null) {
                    $scope.$apply(function () {
                        var count = JSON.parse(message.body).count;
                        $scope.counter.update(count);
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

            $scope.eventBus.registerHandler('public-orders-canceled', function (error, message) {
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

                        var p1 = $scope.coffeeChart.data.datasets[0].data;
                        p1.splice(0, p1.length);
                        Array.prototype.push.apply(p1, products['1']);
                        $scope.coffeeChart.update();

                        var p2 = $scope.milkChart.data.datasets[0].data;
                        p2.splice(0, p2.length);
                        Array.prototype.push.apply(p2, products['2']);
                        $scope.milkChart.update();

                        var p3 = $scope.breadChart.data.datasets[0].data;
                        p3.splice(0, p3.length);
                        Array.prototype.push.apply(p3, products['3']);
                        $scope.breadChart.update();
                    });
                } else {
                    console.log(error);
                }
            });
        };

    }]);
