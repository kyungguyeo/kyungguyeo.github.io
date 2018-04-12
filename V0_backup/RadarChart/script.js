var w = 400,
    h = 400;

var colorscale = d3.scale.category10();

//Legend titles
var LegendOptions = ['COPD','Type II Diabetes', 'Lung Cancer'];

//Data
var d = [
          [
            {axis:"Education",value:0.59},
            {axis:"Food",value:0.56},
            {axis:"Employment",value:0.42},
            {axis:"Housing",value:0.34}
          ],[
            {axis:"Education",value:0.48},
            {axis:"Food",value:0.41},
            {axis:"Employment",value:0.27},
            {axis:"Housing",value:0.28}
          ], [
            {axis:"Education",value:0.99},
            {axis:"Food",value:0.02},
            {axis:"Employment",value:0.16},
            {axis:"Housing",value:0.85}
          ]
        ];

//Options for the Radar chart, other than default
var mycfg = {
  w: w,
  h: h,
  maxValue: 0.6,
  levels: 6,
  ExtraWidthX: 300
}

//Call function to draw the Radar chart
//Will expect that data is in %'s
RadarChart.draw("#chart", d, mycfg);

////////////////////////////////////////////
/////////// Initiate legend ////////////////
////////////////////////////////////////////

var svg = d3.select('#body')
    .selectAll('svg')
    .append('svg')
    .attr("width", w+100)
    .attr("height", h)

//Create the title for the legend
var text = svg.append("text")
    .attr("class", "title")
    .attr('transform', 'translate(90,0)') 
    .attr("x", w - 70)
    .attr("y", 10)
    .attr("font-size", "12px")
    .attr("fill", "#404040")
    .text("Diseases");
        
//Initiate Legend   
var legend = svg.append("g")
    .attr("class", "legend")
    .attr("height", 50)
    .attr("width", 100)
    .attr('transform', 'translate(90,20)') 
    ;
    //Create colour squares
    legend.selectAll('rect')
      .data(LegendOptions)
      .enter()
      .append("rect")
      .attr("x", w - 65)
      .attr("y", function(d, i){ return i * 20;})
      .attr("width", 10)
      .attr("height", 10)
      .style("fill", function(d, i){ return colorscale(i);})
      ;
    //Create text next to squares
    legend.selectAll('text')
      .data(LegendOptions)
      .enter()
      .append("text")
      .attr("x", w - 52)
      .attr("y", function(d, i){ return i * 20 + 9;})
      .attr("font-size", "8px")
      .attr("fill", "#737373")
      .text(function(d) { return d; })
      ; 