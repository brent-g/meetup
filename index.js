const { links, api_key } = require('./api-links.js');

const request = require('request');
const express = require('express');
const app = express();
const clone = require('clone');
const qs = require('query-string');

const { Pool, Client } = require('pg');
const pool = new Pool({
  user: 'postgres',
  host: 'localhost',
  database: 'meetup',
  password: 'brentg',
  port: 5432
});

const reducer = (accumulator, currentValue) => accumulator + currentValue;

console.log('server started....');

function init() { 

	var promises = [];

	links.signed.forEach(function(group) {
		var group_name = Object.entries(group)[0][0];
		var group_url = Object.entries(group)[0][1];
		var group_data = new Promise(function(resolve, reject) {
			
			var options = {
				url: group_url,
				json: true
			};

			request(options, function(error, resp, body) {
				if (error) {
					console.log('an error has occured grabbing the data from ' + group_name);
					// console.log('error grabbing data for spontaneous', error);
					return reject(error);
				}

				console.log('Checking: ' + group_name + ' @ ' + new Date());
				return resolve(body);
			});
		});

		group_data.catch(function(error) {
			console.log('FAILED: ' + group_name + ' @ ' + new Date());
		});

		promises.push(group_data);
	});

	Promise.all(promises).then(function(data_obj) {
		console.log('>>>>>> All promises resolved');
		// loop over all of the events and add them to the database if they do not exist already
		data_obj.forEach(function(group) {
			group.forEach(function(event_data) {
			
				var event_object = {
					meetup_event_id : event_data.id,
					name            : event_data.name,
					status          : event_data.status,
					time            : event_data.time,
					local_date      : event_data.local_date,
					local_time      : event_data.local_time,
					yes_rsvp_count  : event_data.yes_rsvp_count,
					group_id        : event_data.group.id,
					urlname         : event_data.group.urlname,
					link            : event_data.link
				};

				existingEvent(event_object, function(exists, resp) {
					// update
					if (exists === true) {
						// check the rsvp numbers and update if the rsvp is different from whats in the db
						if (event_object.yes_rsvp_count !== resp.rows[0].yes_rsvp_count) {
							// the rsvp value in the database is different from the data from the meetup server therefor update the database value
							updateEvent(event_object, function() {
								// update the rvsp values on meetup.com
								updateAllGroupRSVPSByEvent(event_object, function() {
									console.log('RSVP values updated!');
								});
							});
							// console.log('RSVP values have changed!');
						}
						// console.log('event already exists!');

					// insert 
					} else if (exists === false) {
						insertEvent(event_object, function() {
							console.log('Event DOES NOT exist - inserting new record!!');
						});
					} else {
						console.log('existingEvent() Special case was hit');
					}
				});
			});
		});
	}, function() {
		console.log('>>>>>> 1 or more promises rejected');
	});
}

init();

setInterval(function() {
	console.log('-------------------------------------------');
	init();
}, (1000*60*2));

/* Used for Debugging */
var data = null;

app.get('/', function(req, res){
	res.send(data); //replace with your data here
});

app.listen(3000);
/* Used for Debugging */

function existingEvent(event_obj, callback) {
	var select_query = {
		text: `
			select 
				group_id,
				meetup_event_id, 
				name, 
				time, 
				yes_rsvp_count 
			from events 
			where meetup_event_id = $1 
			and name = $2 
			and local_date = $3
			and local_time = $4
			and group_id = $5
			and status = 'upcoming'

			--order by event_id asc`,
		values: [
			event_obj.meetup_event_id, 
			event_obj.name, 
			event_obj.local_date, 
			event_obj.local_time, 
			event_obj.group_id
		]
	};

	pool.query(select_query, function(error, resp) {
		if (error) {
			console.log('existingEvent() Error:', err);
		}

		if (resp.rowCount > 0) {
			callback(true, resp);
		} else {
			callback(false, resp);
		}
	});
}

function insertEvent(event_obj, callback) {
	var insert = {
		text: `
		insert into events 
		(
			"meetup_event_id", 
			"name", 
			"status", 
			"time", 
			"local_date", 
			"local_time", 
			"yes_rsvp_count",
			"group_id",
			"urlname",
			"link"
		) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);`,
		values: [
			event_obj.meetup_event_id, 
			event_obj.name, 
			event_obj.status, 
			event_obj.time, 
			event_obj.local_date, 
			event_obj.local_time,
			event_obj.yes_rsvp_count,
			event_obj.group_id,
			event_obj.urlname,
			event_obj.link
		]
	}

	pool.query(insert, function(error, resp) {
		if (error) {
			console.log('insertEvent() Error:', error);
		} else {
			callback();
		}
	});
}

function updateEvent(event_obj, callback) {
	var update = {
		text: `
			update 
				events 
			set yes_rsvp_count = $1
				where 
			meetup_event_id = $2
			and name = $3
			and status = $4
			and local_date = $5
			and local_time = $6
			and group_id = $7;`,
		values: [
			event_obj.yes_rsvp_count,
			event_obj.meetup_event_id, 
			event_obj.name, 
			event_obj.status, 
			event_obj.local_date, 
			event_obj.local_time,
			event_obj.group_id
		]
	}

	pool.query(update, function(error, resp) {
		if (error) {
			console.log('updateEvent() Error:', err);
		} else {
			callback();
		}
	});
}

function updateRSVPCount(event_obj, callback) {

	var url_param_obj = {
		sign     : true,
		key      : api_key,
		response : 'yes',
		guests   : event_obj.guest_rsvps
	}

	var url_params = qs.stringify(url_param_obj, { sort: false });
	var url = 'https://api.meetup.com/' + event_obj.urlname + '/events/' + event_obj.event_id + '/rsvps?' + url_params;

	var options = {
		url: url,
		format: 'json'
	}
	
	// on success this returns data about the event
	request.post(options, function(error, resp, body) {
		if (error) {
			console.log('updateRSVPCount() Error posting to meetup api');
		} else {
			callback(event_obj);
		}
	});
}

function updateAllGroupRSVPSByEvent(event_obj, callback) {
	// this query has a -1 for the rsvp count to compensate for the host being included in the data
	// we need to make sure that we only update rvsp values if multiple groups have the same event
	// if we dont then we will end up overwriting the rvsp value with the below functionality -- rsvps_copy[index] = 0
	// therefore we add the having clause array_length(array_agg(meetup_event_id order by event_id asc), 1) > 1 to remove any
	// events that arent being hosted across multiple groups
	var select_query = {
		text: `
		select
		array_to_json(array_agg(meetup_event_id order by event_id asc)) as event_ids,
		array_to_json(array_agg(urlname order by event_id asc)) as urlnames,
		array_to_json(array_agg(yes_rsvp_count - 1 order by event_id asc)) as rsvps,
		name,
		local_date,
		local_time,
		sum(yes_rsvp_count)
		from events
		where 
			local_date >= now() - interval '1 day'
			and name = $1
			and local_date = $2
			and local_time = $3
		group by name, local_date, local_time
		having array_length(array_agg(meetup_event_id order by event_id asc), 1) > 1
		order by local_date asc, local_time asc
			`,
		values: [
			event_obj.name,
			event_obj.local_date,
			event_obj.local_time
		]
	};

	pool.query(select_query, function(error, resp) {
		if (error) {
			console.log('updateAllGroupRSVPSByEvent() Error:', error);
		}

		if (resp.rowCount > 0) {
			resp.rows.forEach(function(row, index, arr) {
				console.log('vvvvvvvvvvvvvvvvvvvvv');
				row.event_ids.forEach(function(event_id, index, arr) {
					// deep clone so the object values don't change when we overwrite the array index value
					var rsvps_copy = clone(row.rsvps);
					rsvps_copy[index] = 0;
					var guest_rsvps = rsvps_copy.reduce(reducer);
					// we are limited to a maximum of 99 guests
					if (guest_rsvps > 99) {
						guest_rsvps = 99;
					}

					// data that the updateRSVPCount function is expecting
					var rsvp_object = {
						event_id    : event_id,
						urlname     : row.urlnames[index],
						guest_rsvps : guest_rsvps
					}

					// set a time out of 3 seconds so we don't spam the meetup api server
					setTimeout(function() {
						updateRSVPCount(rsvp_object, function(data){
							console.log('Success updating RSVP: ', data.guest_rsvps, data.urlname, data.event_id);
						});
					}, index * 3000);
				});
				console.log('^^^^^^^^^^^^^^^^^^^^^');
			});
			callback(true, resp);
		} else {
			callback(false, resp);
		}
	});
}

function saveGroups(callback) {

	var select = {
		text: `
			select 
				group_id, 
				name,
				urlname
			from groups
			`,
		values: []
	}

	pool.query(select, function(error, resp) {
		if (error) {
			console.log('saveGroups() Select - Error:', error);
		} else {
			
			var rows = resp.rows;

			var options = {
				url: links.my_groups,
				json: true
			};

			request(options, function(error, resp, body) {
				if (error) {
					console.log('saveGroups(); Error', error);
				}

				var active_groups = body.filter(function(element) {
					return element.status === 'active';
				});

				active_groups.forEach(function(group_obj) {

					var insert = {
						text: `
							insert into groups (
								"group_id", 
								"name", 
								"urlname"
							) values ($1, $2, $3)`,
						values: [
							group_obj.id, 
							group_obj.name, 
							group_obj.urlname
						]
					}

					pool.query(insert, function(error, resp) {
						if (error) {
							console.log('saveGroups() Error:', error);
						} else {
							
							// console.log(res.rows[0])
						}
					});
				});

				data = active_groups;

				callback(body);
			});
		}
	});
}