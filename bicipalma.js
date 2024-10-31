const neo4j = require('neo4j-driver');
const axios = require('axios');
const fs = require('fs');

const biciPalma = async () => {

  const driver = neo4j.driver(
    'bolt://localhost:7687/routes',
    neo4j.auth.basic('neo4j', 'password')
  );

  const session = driver.session({ database:'bicipalma'});
  let stations = await axios.get(`https://maps.nextbike.net/maps/nextbike-official.json?city=789`);

  for ( let place of stations.data.countries[0].cities[0].places ) {
        
    let result = await session.run(
        `MATCH (station:Station {id: $id}) RETURN station`,
        {
            id: place.uid
        }
    );

    let station = result.records[0];

    if (station && station.get("station").properties.maintenance !== place.maintenance && place.maintenance === true) {
          
        await session.run(
            `MERGE (station:Station {id: $id})
            SET station.maintenance = true,
            station.maintenanceHistory = station.maintenanceHistory + {startTime: timestamp()}`,
            {
                id: place.uid,
            }
        );              
    }
    
    else if ( station && station.get("station").properties.maintenance !== undefined && station.get("station").properties.maintenance !== place.maintenance && place.maintenance === false) {

        const endTime = new Date().getTime();
        const startTime = station.get("station").properties.maintenanceHistory[-1].startTime;
        const duration = endTime - startTime;

        await session.run(
            `MATCH (station:Station {id: $id})
            SET station.maintenance = false,
            station.maintenanceHistory[-1].endTime = $endTime,
            station.maintenanceHistory[-1].duration = $duration`,
            {
                id: place.uid,
                endTime: endTime,
                duration: duration
            }
        );
    }

    else if (station && station.get("station").properties.maintenance === undefined) {

        await session.run(
            `MATCH (station:Station {id: $id})
            SET station.maintenance = false`,
            {
                id: place.uid
            }
        );
    }

    if (station && station.get("station").properties.bikesAvailableToRent > place.bikes_available_to_rent && place.bikes_available_to_rent === 0) {

        await session.run(
            `MERGE (station:Station {id: $id})
            SET station.notAvailableToRentHistory = station.notAvailableToRentHistory + {startTime: timestamp()}`,
            {
                id: place.uid,
            }
        );   

    }else if (station && station.get("station").properties.bikesAvailableToRent === 0 && place.bikes_available_to_rent > 0) {

        const endTime = new Date().getTime();
        const startTime = station.get("station").properties.notAvailableToRentHistory[-1].startTime;
        const duration = endTime - startTime;

        await session.run(
            `MATCH (station:Station {id: $id})
            SET station.notAvailableToRentHistory[-1].endTime = $endTime,
            station.notAvailableToRentHistory[-1].duration = $duration`,
            {
                id: place.uid,
                endTime: endTime,
                duration: duration
            }
        );
    }

    if (station && station.get("station").properties.freeRacks > place.free_racks && place.free_racks === 0) {

        await session.run(
            `MERGE (station:Station {id: $id})
            SET station.notAvailableRacksHistory = station.notAvailableRacksHistory + {startTime: timestamp()}`,
            {
                id: place.uid,
            }
        );
    }
    else if (station && station.get("station").properties.freeRacks === 0 && place.free_racks > 0) {

        const endTime = new Date().getTime();
        const startTime = station.get("station").properties.notAvailableRacksHistory[-1].startTime;
        const duration = endTime - startTime;

        await session.run(
            `MATCH (station:Station {id: $id})
            SET station.notAvailableRacksHistory[-1].endTime = $endTime,
            station.notAvailableRacksHistory[-1].duration = $duration`,
            {
                id: place.uid,
                endTime: endTime,
                duration: duration
            }
        );
    }

    await session.run(
        `MERGE (station:Station {id: $id})
        SET station.name = $name,
        station.y = $y,
        station.x = $x,
        station.bikeRacks = $bikeRacks,
        station.bikes = $bikes,
        station.bikesAvailableToRent = $bikesAvailableToRent,
        station.freeRacks = $freeRacks
        RETURN station`,
        {
            id: place.uid,
            name: place.name,
            y: place.lat,
            x: place.lng,
            bikeRacks: place.bike_racks,
            bikes: place.bikes,
            bikesAvailableToRent: place.bikes_available_to_rent,
            freeRacks: place.free_racks
        }
    );

    for ( let bike of place.bike_list ) {

        let result = await session.run(
            `MATCH (bike:Bike {id: $id}) RETURN bike`,
            {
                id: bike.number
            }
        );

        let lastStateBike = result.records[0];

        if (lastStateBike !== undefined && lastStateBike.get("bike").properties.currentStation !== place.uid) {

            let endTime = new Date().getTime();
            let startTime = lastStateBike.get("bike").properties.lastUpdate;
            let duration = endTime - startTime;
            let cost;

            if (duration <= 30 * 60 * 1000) {
                cost = 0;
            } else if (duration <= 2 * 60 * 60 * 1000) {
                cost = 0.5 * Math.ceil(duration / (30 * 60 * 1000));
            } else {
                cost = 1.5 + (duration - 2 * 60 * 60 * 1000) / (30 * 60 * 1000) * 3;
            }

            if(bike.bike_type === 150) {

                await session.run(
                    `MATCH (start:Station), (end:Station), (bike:Bike)
                    WHERE (start.id = $startId AND end.id = $endId AND bike.id = $bikeNumber)
                    CREATE (travel:Travel {id: $travelId})
                    SET travel.startTime = $startTime,
                        travel.endTime = $endTime,
                        travel.duration = $duration,
                        travel.cost = $cost 
                    CREATE (travel)-[:STARTED_AT]->(start)
                    CREATE (travel)-[:ENDED_AT]->(end)
                    CREATE (travel)-[:TRAVELLED_BY]->(bike)
                    CREATE (bike)-[:TRAVELLED_ON]->(travel)`,
                    {
                        startId: lastStateBike.get("bike").properties.currentStation,
                        endId: place.uid,
                        bikeNumber: bike.number,
                        travelId: `${bike.number}-${startTime}`,
                        startTime: startTime,
                        endTime: endTime,
                        duration: duration,
                        cost: cost
                    }
                );

            }else if(bike.bike_type === 143) {

                await session.run(
                    `MATCH (start:Station), (end:Station), (bike:Bike)
                    WHERE (start.id = $startId AND end.id = $endId AND bike.id = $bikeNumber)
                    CREATE (travel:Travel {id: $travelId})
                    SET travel.startTime = $startTime,
                        travel.endTime = $endTime,
                        travel.batteryConsumed = $batteryConsumed,
                        travel.duration = $duration
                    CREATE (travel)-[:STARTED_AT]->(start)
                    CREATE (travel)-[:ENDED_AT]->(end)
                    CREATE (travel)-[:TRAVELLED_BY]->(bike)
                    CREATE (bike)-[:TRAVELLED_ON]->(travel)`,
                    {
                        startId: lastStateBike.get("bike").properties.currentStation,
                        endId: place.uid,
                        bikeNumber: bike.number,
                        travelId: `${bike.number}-${startTime}`,
                        startTime: startTime,
                        endTime: endTime,
                        batteryConsumed: lastStateBike.get("bike").properties.battery - bike.pedelec_battery,
                        duration: duration
                    }
                );
            }
        }

        if (lastStateBike !== undefined && lastStateBike.get("bike").properties.active !== bike.active && bike.active === false) {
          await session.run(
            `MERGE (bike:Bike {id: $id})
              bike.stateHistory = bike.stateHistory + {
                  active: $active,
                  startTime: timestamp(),
                  state: $state,
                  station: $station
              }
            }`,
            {
              id: bike.number,
              active: bike.active,
              state: bike.state,
              station: place.uid
            }
          );
        }
        
        else if (lastStateBike !== undefined && lastStateBike.get("bike").properties.active !== bike.active && bike.active === true) {

          const endTime = new Date().getTime();
          const startTime = lastStateBike.get("bike").properties.stateHistory[-1].startTime;
          const duration = endTime - startTime;

          await session.run(
            `MERGE (bike:Bike {id: $id})
            SET bike.stateHistory[-1].endTime = $endTime,
              bike.stateHistory[-1].duration = $duration`,
            {   
              id: bike.number,
              endTime: endTime,
              duration: duration
            }               
          );
        }

      await session.run(
        `MERGE (bike:Bike {id: $id})
        SET bike.bikeType = $bikeType,
          bike.battery = $battery,
          bike.active = $active,
          bike.state = $state,
          bike.currentStation = $currentStation,
          bike.lastUpdate = timestamp()
        RETURN bike`,
        {
          id: bike.number,
          bikeType: bike.bike_type,
          battery: bike.pedelec_battery,
          active: bike.active,
          state: bike.state,
          currentStation: place.uid,
        }
      );
    }
  }  

  const currentDate = new Date();
  console.log(`Done at ${currentDate.toString()}`);
}

try {
  setInterval(biciPalma, 60000);
} catch (error) {
  const currentDate = new Date();
  const errorMessage = `[${currentDate.toString()}] ${error.message}`;
  fs.writeFileSync('error.log', errorMessage);
}